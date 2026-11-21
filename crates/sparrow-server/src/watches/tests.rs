// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Real owned-kernel queues exercise the production detector's coverage epoch.

use super::*;

thread_local! {
    /// Worker passes run on this test thread, counted by `run`.
    pub(super) static PASSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[test]
fn kernel_overflow_and_directory_exhaustion_require_fresh_coverage() {
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let original = producer.join("finished.bin");
    std::fs::write(&original, b"finished source\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = Detector::open(
        store.clone(),
        ManagedWatch {
            definition: ConsumeWatch {
                id: "kernel-loss".into(),
                root: producer.clone(),
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: "kernel-loss".into(),
                format: "opaque".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 2,
                publish_hold: false,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Stored,
    )
    .unwrap();
    detector.scan_step().unwrap();
    detector.drain().unwrap();
    assert!(detector.covered);

    // Read-only OPEN restarts observation, but CLOSE_NOWRITE does not turn the
    // input into a completed source or change the observed stability clock.
    let reader = File::open(&original).unwrap();
    detector.drain().unwrap();
    let opened = detector.candidates[Path::new("finished.bin")].stable_since;
    drop(reader);
    detector.drain().unwrap();
    assert_eq!(
        detector.candidates[Path::new("finished.bin")].stable_since,
        opened
    );
    assert!(detector.ready().unwrap().is_none());

    // Exercise real writes from two descriptors. Closing one writer gives no
    // authority to ignore later activity from the other. Backdate only the
    // candidate clock; all activity below still traverses the kernel queue.
    use std::io::Write;
    let activity = producer.join("activity.bin");
    std::fs::write(&activity, b"initial").unwrap();
    let mut first_writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&activity)
        .unwrap();
    let mut second_writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&activity)
        .unwrap();
    detector.drain().unwrap();
    detector
        .candidates
        .get_mut(Path::new("activity.bin"))
        .unwrap()
        .stable_since = Instant::now() - Duration::from_secs(60);
    let write_started = Instant::now();
    first_writer.write_all(b" first").unwrap();
    detector.drain().unwrap();
    assert!(detector.candidates[Path::new("activity.bin")].stable_since >= write_started);
    assert!(detector.ready().unwrap().is_none());
    drop(first_writer);
    detector.drain().unwrap();
    detector
        .candidates
        .get_mut(Path::new("activity.bin"))
        .unwrap()
        .stable_since = Instant::now() - Duration::from_secs(60);
    let write_started = Instant::now();
    second_writer.write_all(b" second").unwrap();
    detector.drain().unwrap();
    assert!(detector.candidates[Path::new("activity.bin")].stable_since >= write_started);
    assert!(detector.ready().unwrap().is_none());
    drop(second_writer);
    detector.drain().unwrap();
    assert!(detector.ready().unwrap().is_none());
    std::fs::remove_file(activity).unwrap();
    detector.drain().unwrap();
    assert!(!detector.candidates.contains_key(Path::new("activity.bin")));

    // A file closed outside the watched root is discovered by MOVED_TO. Queued
    // events for its old path cannot erase the surviving renamed generation.
    let staged = directory.path().join("staged.bin");
    std::fs::write(&staged, b"moved complete source").unwrap();
    let arrived = producer.join("arrived.bin");
    std::fs::rename(&staged, &arrived).unwrap();
    let moved = Instant::now();
    detector.drain().unwrap();
    let moved_identity = detector.candidates[Path::new("arrived.bin")].identity;
    assert!(detector.candidates[Path::new("arrived.bin")].stable_since >= moved);
    assert!(detector.ready().unwrap().is_none());
    let renamed = producer.join("renamed.bin");
    std::fs::rename(&arrived, &renamed).unwrap();
    std::fs::write(&arrived, b"temporary replacement").unwrap();
    std::fs::remove_file(&arrived).unwrap();
    detector.drain().unwrap();
    assert!(!detector.candidates.contains_key(Path::new("arrived.bin")));
    let renamed_identity = detector.candidates[Path::new("renamed.bin")].identity;
    assert_eq!(renamed_identity.device, moved_identity.device);
    assert_eq!(renamed_identity.inode, moved_identity.inode);
    assert_eq!(std::fs::read(&renamed).unwrap(), b"moved complete source");
    std::fs::remove_file(renamed).unwrap();
    detector.drain().unwrap();
    assert_eq!(detector.candidates.len(), 1);
    assert!(store.consumption_pending().unwrap().is_empty());
    assert!(store.records(crate::now().unwrap()).unwrap().is_empty());

    let candidate = detector
        .candidates
        .get_mut(Path::new("finished.bin"))
        .unwrap();
    candidate.stable_since = Instant::now() - Duration::from_secs(60);
    assert!(detector.ready().unwrap().is_some());

    // An independent kernel queue witnesses genuine IN_Q_OVERFLOW. Sparrow's
    // earlier EVENT_QUANTUM reset may discard its own overflow marker, which is
    // deliberately the same loss-of-coverage path, not permission to keep ages.
    let capacity: usize = std::fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        (1..=65_536).contains(&capacity),
        "kernel queue capacity {capacity} exceeds this bounded fixture's 65,536-event profile"
    );
    let witness = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK).unwrap();
    inotify::add_watch(
        &witness,
        &original,
        WatchFlags::OPEN | WatchFlags::CLOSE_NOWRITE,
    )
    .unwrap();
    for _ in 0..=capacity {
        drop(File::open(&original).unwrap());
    }
    let mut buffer = [MaybeUninit::uninit(); 4096];
    let mut reader = inotify::Reader::new(witness, &mut buffer);
    let mut overflow = 0;
    let mut events = 0;
    loop {
        match reader.next() {
            Ok(event) => {
                events += 1;
                assert!(
                    events <= capacity + 1,
                    "witness exceeded the captured queue bound"
                );
                overflow += usize::from(event.events().contains(ReadFlags::QUEUE_OVERFLOW));
            }
            Err(error) if error == rustix::io::Errno::WOULDBLOCK => break,
            Err(error) => panic!("read owned overflow witness: {error}"),
        }
    }
    assert_eq!(overflow, 1, "no actual kernel overflow observed");
    let loss = Instant::now();
    detector.drain().unwrap();
    assert_eq!(detector.coverage_losses, 1);
    assert_eq!(detector.reconciliations, 2);
    assert!(!detector.covered && detector.candidates.is_empty() && detector.renames.is_empty());
    assert!(detector.ready().unwrap().is_none());
    let registry = WatchRegistry::default();
    registry
        .update(std::slice::from_ref(&detector), &[], &[])
        .unwrap();
    let status = registry.snapshot().unwrap().pop().unwrap();
    assert!(!status.covered);
    assert_eq!(status.coverage_losses, 1);
    assert_eq!(status.candidates, 0);

    detector.scan_step().unwrap();
    detector.drain().unwrap();
    assert!(detector.covered);
    assert!(detector.candidates[Path::new("finished.bin")].stable_since >= loss);
    assert!(
        detector.ready().unwrap().is_none(),
        "stale pre-overflow age survived reconciliation"
    );
    assert!(store.consumption_pending().unwrap().is_empty());
    assert!(store.records(crate::now().unwrap()).unwrap().is_empty());
    assert_eq!(std::fs::read(&original).unwrap(), b"finished source\n");

    // Exhaust Sparrow's configured directory budget, not the host's shared
    // inotify quota. The real scan must fail before permitting source claims.
    let nested = producer.join("nested");
    std::fs::create_dir(&nested).unwrap();
    std::fs::write(nested.join("second.bin"), b"second finished source\n").unwrap();
    detector.config.max_directories = 1;
    detector
        .reconcile("bounded directory exhaustion fixture")
        .unwrap();
    let error = detector.scan_step().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("directory coverage limit exceeded")
    );
    assert!(!detector.covered);
    assert!(detector.ready().unwrap().is_none());
    assert!(store.consumption_pending().unwrap().is_empty());
    assert_eq!(std::fs::read(&original).unwrap(), b"finished source\n");
    assert_eq!(
        std::fs::read(nested.join("second.bin")).unwrap(),
        b"second finished source\n"
    );

    detector.config.max_directories = 2;
    detector
        .reconcile("explicit directory budget repair")
        .unwrap();
    detector.scan_step().unwrap();
    detector.drain().unwrap();
    assert!(detector.covered);
    assert_eq!(detector.directories.len(), 2);
    assert_eq!(detector.candidates.len(), 2);
    assert!(detector.ready().unwrap().is_none());
    assert!(store.records(crate::now().unwrap()).unwrap().is_empty());
    eprintln!(
        "PASS actual kernel overflow: capacity={capacity}, witnessed_events={events}, loss_counter=1; fresh readiness and bounded-directory repair"
    );
}

/// INVARIANT (#417): the retired-claim record warns at three quarters of its bound and again
/// at the bound, and not before.
#[test]
fn the_retired_record_warns_at_three_quarters_and_at_the_bound() {
    let table = [
        (0, RetiredFill::Below),
        (767, RetiredFill::Below),
        (768, RetiredFill::Warn),
        (1023, RetiredFill::Warn),
        (1024, RetiredFill::Full),
    ];
    for (count, fill) in table {
        assert_eq!(RetiredFill::of(count, 1024), fill, "{count}");
    }
}

#[test]
fn failing_claim_retries_back_off_to_a_bounded_ceiling() {
    // INVARIANT: a claim that keeps failing slows to one attempt per 15 minutes
    // instead of repeating its publication work every few seconds.
    assert_eq!(retry_delay(1), RETRY_FIRST);
    assert_eq!(retry_delay(2), 2 * RETRY_FIRST);
    assert_eq!(retry_delay(8), 128 * RETRY_FIRST);
    assert_eq!(retry_delay(9), RETRY_CEILING);
    assert_eq!(retry_delay(u32::MAX), RETRY_CEILING);
}

fn mark(parts_done: u32, bytes: u64, stage: u8, prepared: u64) -> ProgressMark {
    ProgressMark {
        parts_done,
        bytes,
        stage,
        prepared,
        raw_chunks: 0,
    }
}

#[test]
fn progress_that_advances_the_mark_resets_the_failure_count() {
    // INVARIANT (#280): an attempt that moved past the best mark, by parts, bytes, or only by
    // its seal stage or prepared records, starts the backoff again from the first delay.
    for advance in [
        mark(1, 0, 0, 0),
        mark(0, 2, 0, 0),
        mark(0, 1, 1, 0),
        mark(0, 1, 0, 1),
    ] {
        let mut entry = ClaimRetry::due();
        entry.failed(mark(0, 1, 0, 0), false);
        entry.failed(mark(0, 1, 0, 0), false);
        assert_eq!(entry.failures, 2);
        entry.failed(advance, false);
        assert_eq!(entry.failures, 1, "{advance:?}");
        assert_eq!(entry.best, advance);
    }
}

#[test]
fn an_attempt_without_mark_progress_keeps_growing_its_backoff() {
    // INVARIANT (#280): a bare retry, including one that begins a reclaimed part again and so
    // stays below the old high-water mark, never resets the failure count.
    let mut entry = ClaimRetry::due();
    entry.failed(mark(3, 10, 2, 5), false);
    for failures in 2..6 {
        entry.failed(mark(2, 99, 2, 9), false);
        assert_eq!(entry.failures, failures);
        assert!(entry.at > Instant::now() + retry_delay(failures - 1));
    }
    assert_eq!(entry.best, mark(3, 10, 2, 5));
}

#[test]
fn a_floor_refusal_keeps_the_claims_cache_and_best_mark() {
    // INVARIANT (#280, F3): a claim has its retry entry before its first attempt, and a
    // floor-class failure keeps that entry due now, with its failures, best mark and shared
    // parts unchanged, so the first pass after the resume edge reuses them.
    let mut entry = ClaimRetry::due();
    assert!(entry.at <= Instant::now(), "due before its first attempt");
    let attempt = Arc::clone(&entry.attempt);
    entry.failed(mark(2, 0, 0, 0), false);
    let (failures, best) = (entry.failures, entry.best);
    entry.failed(mark(1, 0, 0, 0), true);
    assert!(entry.at <= Instant::now());
    assert_eq!((entry.failures, entry.best), (failures, best));
    assert!(Arc::ptr_eq(&attempt, &entry.attempt));
}

#[test]
fn periodic_rescan_keeps_quiet_clocks_and_finds_missed_files() {
    // INVARIANT: a periodic scan under continuous coverage never restarts a
    // tracked candidate's quiet interval; it only adds what discovery missed.
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir_all(producer.join("nested")).unwrap();
    std::fs::write(producer.join("finished.bin"), b"finished source\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = Detector::open(
        store,
        ManagedWatch {
            definition: ConsumeWatch {
                id: "rescan".into(),
                root: producer.clone(),
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: "rescan".into(),
                format: "opaque".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 4,
                publish_hold: false,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Stored,
    )
    .unwrap();
    detector.scan_step().unwrap();
    detector.drain().unwrap();
    assert!(detector.covered);
    let quiet_since = Instant::now() - Duration::from_secs(60);
    detector
        .candidates
        .get_mut(Path::new("finished.bin"))
        .unwrap()
        .stable_since = quiet_since;

    // Drop a discovered file from tracking, as a missed discovery race would.
    std::fs::write(producer.join("nested/missed.bin"), b"missed source\n").unwrap();
    detector.drain().unwrap();
    detector.candidates.remove(Path::new("nested/missed.bin"));

    detector.rescan("periodic scan").unwrap();
    assert!(detector.covered);
    assert!(detector.ready().unwrap().is_some());
    detector.scan_step().unwrap();
    detector.drain().unwrap();
    assert_eq!(detector.reconciliations, 1);
    assert_eq!(detector.directories.len(), 2);
    assert_eq!(
        detector.candidates[Path::new("finished.bin")].stable_since,
        quiet_since
    );
    assert!(
        detector
            .candidates
            .contains_key(Path::new("nested/missed.bin"))
    );
    assert_eq!(
        detector.ready().unwrap().map(|(path, _)| path),
        Some(PathBuf::from("finished.bin"))
    );
}

#[test]
fn retiring_a_claim_clears_only_the_failure_that_claim_caused() {
    // INVARIANT (review 3, O12): `retire_claim` clears the watch failure only when the retired
    // claim's own recovery set it. A failure from another claim or from discovery stays.
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detectors = [Detector::open(
        store,
        ManagedWatch {
            definition: ConsumeWatch {
                id: "retire".into(),
                root: producer.clone(),
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: "retire".into(),
                format: "opaque".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 4,
                publish_hold: false,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Stored,
    )
    .unwrap()];
    // Three claimed sources whose private copies are gone, so each may be retired.
    let [cause, other, discovered] = ["cause.bin", "other.bin", "discovered.bin"].map(|name| {
        let path = producer.join(name);
        std::fs::write(&path, name.as_bytes()).unwrap();
        let identity = CompletedFileIdentity::capture(&path.metadata().unwrap());
        let owner = detectors[0].owner.claim().unwrap();
        let journal = owner
            .reserve(Path::new(name), identity, "retire", "opaque")
            .unwrap();
        owner
            .recover(&journal.import_id, crate::now().unwrap())
            .unwrap();
        std::fs::remove_file(producer.join(".sparrow-inflight").join(&journal.import_id)).unwrap();
        journal.import_id
    });
    let retire = |detectors: &mut [Detector], claim: &str| {
        let retirement = watch_admin::ClaimRetirement {
            watch: "retire".into(),
            claim: claim.into(),
            caller: "test".into(),
            reason: "source deleted".into(),
        };
        assert!(retire_claim(detectors, &retirement).is_ok());
    };

    // A failure another claim's recovery set survives retiring this claim.
    detectors[0].fail_claim(&cause, format!("claim {cause}: missing"));
    retire(&mut detectors, &other);
    assert_eq!(
        detectors[0].status_failure().as_deref(),
        Some(format!("claim {cause}: missing").as_str())
    );

    // Retiring the claim that set the failure clears it.
    retire(&mut detectors, &cause);
    assert_eq!(detectors[0].status_failure(), None);

    // A discovery failure names no claim, so no retirement clears it.
    detectors[0].failure = Some("discovery refused".into());
    retire(&mut detectors, &discovered);
    assert_eq!(
        detectors[0].status_failure().as_deref(),
        Some("discovery refused")
    );
}

#[test]
fn a_claim_that_succeeds_leaves_another_claims_failure_in_place() {
    // INVARIANT (#214): the watch failure names one claim; only that claim's own success clears
    // it. Before, any claim's success cleared it, so a claim still unreconciled vanished from
    // the watch's status between its retries and a deploy step read "nothing owed".
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = Detector::open(
        store,
        ManagedWatch {
            definition: ConsumeWatch {
                id: "success".into(),
                root: producer,
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: "success".into(),
                format: "opaque".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 4,
                publish_hold: false,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Stored,
    )
    .unwrap();
    detector.fail_claim(
        "import:aa",
        "claim import:aa: unpublished claimed source is missing".into(),
    );

    detector.clear_failure_of("import:bb");
    assert_eq!(
        detector.status_failure().as_deref(),
        Some("claim import:aa: unpublished claimed source is missing")
    );

    detector.clear_failure_of("import:aa");
    assert_eq!(detector.status_failure(), None);

    // Retiring a claim never clears a failure that names no claim (discovery).
    detector.failure = Some("discovery refused".into());
    detector.clear_failure_of("import:aa");
    assert_eq!(
        detector.status_failure().as_deref(),
        Some("discovery refused")
    );

    // The loop's success path (`record_claim_success`): another claim's success leaves a claim's
    // failure and counts the completion; a failure naming no claim clears on any success, so it
    // does not stay stale for ever once the watch works again.
    detector.failure = None;
    detector.fail_claim("import:aa", "claim import:aa: missing".into());
    detector.record_claim_success("import:bb");
    assert_eq!(
        detector.status_failure().as_deref(),
        Some("claim import:aa: missing")
    );
    assert_eq!(detector.completed, 1);
    detector.record_claim_success("import:aa");
    assert_eq!(detector.status_failure(), None);
    detector.failure = Some("discovery refused".into());
    detector.record_claim_success("import:cc");
    assert_eq!(detector.status_failure(), None);
    assert_eq!(detector.completed, 3);
}

#[test]
fn a_claim_failure_survives_a_claimless_failure_and_another_claims_success() {
    // INVARIANT (#222): the watch keeps a claim's failure and a failure that names no claim
    // apart. Before, the claim-less failure overwrote the claim's, and another claim's success
    // then cleared it, so the claim that was still pending showed no failure in status while
    // its durable pending state stayed. Now status shows the claim failure first and the
    // claim-less one after it, and only the claim's own success or retirement ends the former.
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = Detector::open(
        store,
        ManagedWatch {
            definition: ConsumeWatch {
                id: "both".into(),
                root: producer,
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: "both".into(),
                format: "opaque".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 4,
                publish_hold: false,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Stored,
    )
    .unwrap();
    detector.fail_claim("import:aa", "claim import:aa: source is missing".into());
    detector.failure = Some("reservation refused".into());
    assert_eq!(
        detector.status_failure().as_deref(),
        Some("claim import:aa: source is missing; also: reservation refused")
    );

    // Another claim's success ends the claim-less failure only.
    detector.record_claim_success("import:bb");
    assert_eq!(
        detector.status_failure().as_deref(),
        Some("claim import:aa: source is missing")
    );

    // A later claim-less failure does not displace it either.
    detector.failure = Some("drain refused".into());
    assert_eq!(
        detector.status_failure().as_deref(),
        Some("claim import:aa: source is missing; also: drain refused")
    );
    detector.clear_failure_of("import:aa");
    assert_eq!(detector.status_failure().as_deref(), Some("drain refused"));
}

/// INVARIANT: a private root (`sparrowd --private-intake`) never touches the original root's
/// active tail generations after it opens: reconciliation records no gap for them, and a file
/// below the private root that reuses a foreign generation's (device, inode) is refused, as the
/// store refuses a second active generation of one identity, instead of resuming the foreign one.
#[test]
fn private_tail_root_leaves_foreign_generations_alone() {
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir().unwrap();
    let original = directory.path().join("original");
    let private = directory.path().join("private");
    std::fs::create_dir(&original).unwrap();
    std::fs::create_dir(&private).unwrap();
    let active = private.join("active.log");
    std::fs::write(&active, b"first line\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let (root, file) = (
        std::fs::metadata(&original).unwrap(),
        std::fs::metadata(&active).unwrap(),
    );
    // The original root's active generation, whose file identity the private file reuses.
    let declared = store
        .tail_start(
            &sparrow_core::TailStart {
                request_id: "foreign-active".into(),
                watch: "tail-private".into(),
                watch_revision: 1,
                directories: sparrow_core::ConsumptionDirectories {
                    root_device: root.dev(),
                    root_inode: root.ino(),
                    parent_device: root.dev(),
                    parent_inode: root.ino(),
                },
                path: b"active.log".to_vec(),
                source: "tail-private".into(),
                format: "log".into(),
                device: file.dev(),
                inode: file.ino(),
            },
            crate::now().unwrap(),
        )
        .unwrap();
    let config = ConsumeWatch {
        id: "tail-private".into(),
        root: private.clone(),
        producer_contract: crate::ProducerContract::ActiveFilesRetain,
        source: "tail-private".into(),
        format: "log".into(),
        quiet_ms: 30_000,
        max_directories: 2,
        max_candidates: 2,
        publish_hold: false,
    };
    let mut owner = crate::tailing::TailRoot::open(
        store.clone(),
        &config,
        1,
        crate::consumption::RootBinding::Private,
    )
    .unwrap();
    owner.reconcile_missing("tail-private").unwrap();
    assert!(
        owner.attach(Path::new("active.log"), &config, 1).is_err(),
        "a reused identity is never adopted"
    );
    owner.drain().unwrap();
    let generations = store.tail_generations("tail-private").unwrap();
    assert_eq!(generations.len(), 1);
    assert_eq!(generations[0].id, declared.id);
    assert_eq!(
        generations[0].gap, None,
        "no gap is recorded for the original root's generation"
    );
    assert_eq!(
        generations[0].offset, 0,
        "the foreign generation is never resumed"
    );
}

/// INVARIANT: `sparrowd --private-intake DIR` refuses a DIR that is, holds or lies under a
/// watch's configured root, so a hand run on a live intake directory never reaches the live
/// roots, and a catalog persisted by a private daemon still names the configured roots, so a
/// disposable copy never rewrites where its watches consume. Two configured roots that would
/// share one private root refuse the open rather than lose one on persist.
#[test]
fn private_intake_refuses_configured_roots_and_persists_them_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live").join("intake");
    let configured = live.join("events");
    std::fs::create_dir_all(&configured).unwrap();
    let scratch = directory.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let definition = |root: &Path| ConsumeWatch {
        id: "events".into(),
        root: root.into(),
        producer_contract: crate::ProducerContract::FinishedFiles,
        source: "events".into(),
        format: "jsonl".into(),
        quiet_ms: 1000,
        max_directories: 4,
        max_candidates: 4,
        publish_hold: false,
    };
    // The live intake directory (holds the root), the root itself, and a directory inside it.
    for dir in [live.clone(), configured.clone(), configured.join("nested")] {
        std::fs::create_dir_all(&dir).unwrap();
        let intake = PrivateIntake::new(&dir).unwrap();
        assert!(
            intake.rebind(&mut definition(&configured)).is_err(),
            "{} overlaps the configured root",
            dir.display()
        );
    }
    let intake = PrivateIntake::new(&scratch).unwrap();
    let mut rebound = definition(&configured);
    assert_eq!(intake.rebind(&mut rebound).unwrap(), configured);
    assert_eq!(rebound.root, scratch.canonicalize().unwrap().join("events"));

    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut catalog = Catalog::load(&store, &[definition(&configured)]).unwrap();
    catalog
        .rebind_roots(|definition| intake.rebind(definition))
        .unwrap();
    assert_eq!(catalog.watches()[0].definition.root, rebound.root);
    catalog.persist(&store).unwrap();
    let reloaded = Catalog::load(&store, &[]).unwrap();
    assert_eq!(reloaded.watches()[0].definition.root, configured);

    // Two configured roots with one final name would share a private root, and persisting
    // would lose one of them, so the open refuses and names both.
    let elsewhere = directory.path().join("elsewhere").join("events");
    let mut second = definition(&elsewhere);
    second.id = "elsewhere".into();
    let fresh = directory.path().join("fresh");
    std::fs::create_dir(&fresh).unwrap();
    let data = fresh.join("store");
    let config_path =
        sparrow_test_support::write_server_config(&fresh, &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut shared = Catalog::load(&store, &[definition(&configured), second]).unwrap();
    let refusal = shared
        .rebind_roots(|definition| intake.rebind(definition))
        .unwrap_err()
        .to_string();
    for root in [&configured, &elsewhere] {
        assert!(refusal.contains(&root.display().to_string()), "{refusal}");
    }
}

/// INVARIANT: an existing configured root that is, by device and inode, the private directory,
/// one of its ancestors, the private root itself, or an ancestor-held directory of the private
/// directory is refused however its path is spelled (a bind mount aliases it), while a
/// disjoint root and a root absent on this host are compared by path alone.
#[test]
fn private_intake_refuses_configured_roots_aliased_by_identity() {
    // Chains are (device, inode), the path first and "/" last.
    let dir = [(1, 30), (1, 20), (1, 2)];
    let private = Some((1, 31));
    for (configured, expected) in [
        (vec![(1, 30), (9, 5), (9, 2)], true), // the private directory, by another path
        (vec![(1, 20), (9, 5), (9, 2)], true), // an ancestor of the private directory
        (vec![(1, 31), (9, 5), (9, 2)], true), // the private root DIR/NAME
        (vec![(9, 7), (1, 30), (9, 2)], true), // holds the private directory
        (vec![(9, 7), (9, 5), (1, 2)], false), // shares only "/"
        (vec![], false),                       // absent on this host
    ] {
        assert_eq!(
            super::aliased(&dir, private, &configured),
            expected,
            "{configured:?}"
        );
    }
}

/// INVARIANT: the daemon's own open of a tail file is never a reason to attach it again. When
/// the store refuses an attach after the descriptor is open (here a CONFLICT: the file reuses
/// the identity of a foreign active generation), the refusal's own IN_OPEN must not re-enter
/// `attach`, or the watch reopens the file up to the event quantum per pass and then rescans.
/// An independent kernel watch counts the opens one scan and one drain produce.
#[test]
fn refused_tail_attach_opens_the_file_once() {
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir().unwrap();
    let original = directory.path().join("original");
    let private = directory.path().join("private");
    std::fs::create_dir(&original).unwrap();
    std::fs::create_dir(&private).unwrap();
    let active = private.join("active.log");
    std::fs::write(&active, b"first line\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let (root, file) = (
        std::fs::metadata(&original).unwrap(),
        std::fs::metadata(&active).unwrap(),
    );
    store
        .tail_start(
            &sparrow_core::TailStart {
                request_id: "foreign-active".into(),
                watch: "tail-refused".into(),
                watch_revision: 1,
                directories: sparrow_core::ConsumptionDirectories {
                    root_device: root.dev(),
                    root_inode: root.ino(),
                    parent_device: root.dev(),
                    parent_inode: root.ino(),
                },
                path: b"active.log".to_vec(),
                source: "tail-refused".into(),
                format: "log".into(),
                device: file.dev(),
                inode: file.ino(),
            },
            crate::now().unwrap(),
        )
        .unwrap();

    let counter = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK).unwrap();
    inotify::add_watch(&counter, &private, WatchFlags::OPEN).unwrap();
    let mut detector = Detector::open(
        store,
        ManagedWatch {
            definition: ConsumeWatch {
                id: "tail-refused".into(),
                root: private,
                producer_contract: crate::ProducerContract::ActiveFilesRetain,
                source: "tail-refused".into(),
                format: "log".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 2,
                publish_hold: false,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Private,
    )
    .unwrap();
    detector.scan_step().unwrap();
    detector.drain().unwrap();
    assert!(
        detector.failure.is_some(),
        "the store refused the attach, which is the condition under test"
    );

    let mut buffer = [MaybeUninit::uninit(); 4096];
    let mut reader = inotify::Reader::new(counter, &mut buffer);
    let mut opens = 0;
    while let Ok(event) = reader.next() {
        if event
            .file_name()
            .is_some_and(|name| name.to_bytes() == b"active.log")
        {
            opens += 1;
        }
    }
    assert_eq!(opens, 1, "one refused attach is one open of the file");
}

/// One watch over a fresh root and store, with the handles a test drives its worker by.
struct Worker {
    _directory: tempfile::TempDir,
    store: Store,
    registry: WatchRegistry,
    stop: tokio::sync::watch::Sender<bool>,
    controller: watch_admin::WatchController,
}

/// Open one watch of CONTRACT over a root holding FILES and return its unstarted worker
/// loop, which shares OPERATIONS as its operation budget.
fn worker(
    contract: crate::ProducerContract,
    files: &[(&str, &[u8])],
    operations: Arc<tokio::sync::Semaphore>,
) -> (Worker, impl std::future::Future<Output = Result<(), Error>>) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("producer");
    std::fs::create_dir(&root).unwrap();
    for (name, bytes) in files {
        std::fs::write(root.join(name), bytes).unwrap();
    }
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let definition = ConsumeWatch {
        id: "wake".into(),
        root,
        producer_contract: contract,
        source: "wake".into(),
        format: "log".into(),
        quiet_ms: 30_000,
        max_directories: 2,
        max_candidates: 2,
        publish_hold: false,
    };
    let catalog = Catalog::load(&store, std::slice::from_ref(&definition)).unwrap();
    let detectors = catalog
        .watches()
        .iter()
        .cloned()
        .map(|watch| Detector::open(store.clone(), watch, RootBinding::Stored).unwrap())
        .collect();
    let (controller, commands) = watch_admin::channel();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let registry = WatchRegistry::default();
    let run = run(
        store.clone(),
        Roster {
            detectors,
            unopened: Vec::new(),
        },
        data.join("rpc.sock"),
        stopped,
        registry.clone(),
        (catalog, Backoff::default()),
        Control {
            commands,
            operations,
            private: None,
            outputs: crate::config::OutputPaths::default(),
        },
    );
    (
        Worker {
            _directory: directory,
            store,
            registry,
            stop,
            controller,
        },
        run,
    )
}

fn passes() -> u64 {
    PASSES.with(std::cell::Cell::get)
}

// These tests run the worker on a paused tokio clock, which advances only when every task
// waits on a timer. A worker that slept on a tick would let the clock advance to each tick
// and count a pass there; one blocked on events does neither. Each test acts at SETTLED, off
// the old 50 ms grid, so a ticking worker would still be asleep and need the clock to
// advance. The candidate quiet clocks and rescan deadlines are std instants, so each fixture
// stays below its first rescan (60 seconds) and holds no consume candidate.

/// Long enough for a fresh worker to cover its root, and not a multiple of 50 ms.
const SETTLED: Duration = Duration::from_millis(1025);

/// INVARIANT (#184): an idle watch worker blocks on its wake sources, so it runs at most
/// one pass a second, where the 50 ms tick it replaced ran 20.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn an_idle_worker_wakes_at_most_once_a_second() {
    let (worker, run) = worker(
        crate::ProducerContract::FinishedFiles,
        &[],
        Arc::new(tokio::sync::Semaphore::new(2)),
    );
    tokio::task::LocalSet::new()
        .run_until(async {
            let task = tokio::task::spawn_local(run);
            tokio::time::sleep(SETTLED).await;
            assert!(worker.registry.snapshot().unwrap()[0].covered);
            let settled = passes();
            tokio::time::sleep(Duration::from_secs(30)).await;
            let idle = passes() - settled;
            assert!(idle <= 30, "{idle} passes in 30 idle seconds");
            worker.stop.send_replace(true);
            task.await.unwrap().unwrap();
        })
        .await;
}

/// INVARIANT (#257): a watch paused below the free-space floor waits for the floor's resume
/// edge, an event, and owns no recheck timer (the one it had read free space every second).
/// Protects: the worker records the pause through the floor's ledger, and the observation that
/// sees the pause end, whichever duty makes it, wakes the worker to run a pass with no clock
/// passing. Fails when the worker rechecks on a timer instead: the pass then waits for time.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_paused_watch_wakes_on_the_floor_resume_edge_with_no_clock_passing() {
    let (worker, run) = worker(
        crate::ProducerContract::FinishedFiles,
        &[],
        Arc::new(tokio::sync::Semaphore::new(2)),
    );
    let floor = worker.store.space_floor().clone();
    floor.set_free_for_tests(Some(0));
    tokio::task::LocalSet::new()
        .run_until(async {
            let task = tokio::task::spawn_local(run);
            tokio::time::sleep(SETTLED).await;
            assert!(
                floor.paused(),
                "the watch did not observe the pause through the floor"
            );
            let paused = passes();
            let resumed_at = tokio::time::Instant::now();
            // Another duty ends the pause: this observation consumes the edge.
            floor.set_free_for_tests(None);
            assert!(floor.observe(crate::now().unwrap()).unwrap().is_none());
            let began = std::time::Instant::now();
            while passes() == paused {
                assert!(
                    began.elapsed() < Duration::from_secs(30),
                    "the paused watch did not wake on the resume edge"
                );
                tokio::task::yield_now().await;
            }
            assert_eq!(tokio::time::Instant::now(), resumed_at);
            worker.stop.send_replace(true);
            task.await.unwrap().unwrap();
        })
        .await;
}

/// INVARIANT (#184): a stop request wakes an idle worker at once; no clock has to pass, as
/// the 50 ms stop poll needed.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_stop_request_ends_an_idle_worker_without_waiting_for_a_clock() {
    let (worker, run) = worker(
        crate::ProducerContract::FinishedFiles,
        &[],
        Arc::new(tokio::sync::Semaphore::new(2)),
    );
    tokio::task::LocalSet::new()
        .run_until(async {
            let task = tokio::task::spawn_local(run);
            tokio::time::sleep(SETTLED).await;
            let requested = tokio::time::Instant::now();
            worker.stop.send_replace(true);
            task.await.unwrap().unwrap();
            assert_eq!(tokio::time::Instant::now(), requested);
        })
        .await;
}

/// INVARIANT (#184): an administrative command wakes an idle worker at once, and its reply
/// arrives with no clock passing.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_command_wakes_an_idle_worker_without_waiting_for_a_clock() {
    let (worker, run) = worker(
        crate::ProducerContract::FinishedFiles,
        &[],
        Arc::new(tokio::sync::Semaphore::new(2)),
    );
    tokio::task::LocalSet::new()
        .run_until(async {
            let task = tokio::task::spawn_local(run);
            tokio::time::sleep(SETTLED).await;
            let sent = tokio::time::Instant::now();
            let refused = worker
                .controller
                .retire_claim(watch_admin::ClaimRetirement {
                    watch: "absent".into(),
                    claim: "absent".into(),
                    caller: "test".into(),
                    reason: "wake".into(),
                })
                .await
                .unwrap_err();
            assert_eq!(refused.code, "NOT_FOUND");
            assert_eq!(tokio::time::Instant::now(), sent);
            worker.stop.send_replace(true);
            task.await.unwrap().unwrap();
        })
        .await;
}

/// INVARIANT (#184): a tail drain waiting for the shared operation budget runs as soon as a
/// permit is released, not at the next tick; the input is captured within one millisecond
/// of clock time.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_released_permit_wakes_a_waiting_tail_drain() {
    let operations = Arc::new(tokio::sync::Semaphore::new(1));
    let lease = operations.clone().try_acquire_owned().unwrap();
    let (worker, run) = worker(
        crate::ProducerContract::ActiveFilesRetain,
        &[("current.log", b"n\n")],
        operations,
    );
    tokio::task::LocalSet::new()
        .run_until(async {
            let task = tokio::task::spawn_local(run);
            tokio::time::sleep(SETTLED).await;
            let state = worker.registry.snapshot().unwrap().pop().unwrap();
            assert!(state.tail_admission_waiting && state.tail_descriptors == 1);
            assert_eq!(state.tail_checkpoint_records, 0);
            drop(lease);
            tokio::time::sleep(Duration::from_millis(1)).await;
            let state = worker.registry.snapshot().unwrap().pop().unwrap();
            assert!(!state.tail_admission_waiting && state.failure.is_none());
            assert_eq!(state.tail_checkpoint_records, 1);
            assert_eq!(
                worker
                    .store
                    .tail_generation(&state.tail_generation_ids[0])
                    .unwrap()
                    .offset,
                2
            );
            worker.stop.send_replace(true);
            task.await.unwrap().unwrap();
        })
        .await;
}

/// INVARIANT (#184): the drain before a wait can take producer events from the pass. The
/// worker reports what they changed, and freed candidate capacity for deferred inputs makes
/// the deferred-capacity rescan due at once instead of at the next rescan or quiet deadline.
#[tokio::test(flavor = "current_thread")]
async fn the_drain_before_a_wait_reports_and_wakes_for_what_it_found() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("producer");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("first.bin"), b"first\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detectors = vec![
        Detector::open(
            store.clone(),
            ManagedWatch {
                definition: ConsumeWatch {
                    id: "settle".into(),
                    root: root.clone(),
                    producer_contract: crate::ProducerContract::FinishedFiles,
                    source: "settle".into(),
                    format: "opaque".into(),
                    quiet_ms: 30_000,
                    max_directories: 2,
                    max_candidates: 1,
                    publish_hold: false,
                },
                revision: 1,
                enabled: true,
            },
            RootBinding::Stored,
        )
        .unwrap(),
    ];
    detectors[0].scan_step().unwrap();
    detectors[0].drain().unwrap();
    assert!(detectors[0].covered && detectors[0].candidates.len() == 1);
    // During a pass: a second input finds the capacity full, then the first one goes away.
    std::fs::write(root.join("second.bin"), b"second\n").unwrap();
    std::fs::remove_file(root.join("first.bin")).unwrap();

    let registry = WatchRegistry::default();
    let (_controller, commands) = watch_admin::channel();
    let mut control = Control {
        commands,
        operations: Arc::new(tokio::sync::Semaphore::new(1)),
        private: None,
        outputs: crate::config::OutputPaths::default(),
    };
    // Stop is already requested, so the wait returns once it has settled.
    let (_stop, mut stopped) = tokio::sync::watch::channel(true);
    let waiting = Waiting {
        store: &store,
        registry: &registry,
        unopened: &[],
        backoffs: [&Backoff::default(), &Backoff::default()],
        stop: &mut stopped,
        control: &mut control,
        received: &mut None,
        permit: &mut None,
        resumed: &tokio::sync::Notify::new(),
    };
    pause(waiting, &mut detectors, true, false, true, Wake::default())
        .await
        .unwrap();
    let status = registry.snapshot().unwrap().pop().unwrap();
    assert!(status.deferred && status.candidates == 0);
    assert!(detectors[0].due(true).unwrap() <= Instant::now());
    assert!(detectors[0].due(false).unwrap() > Instant::now());
}

/// A tail detector over a private root holding one file, opened on a fresh store.
fn tail_detector() -> (tempfile::TempDir, Store, Detector) {
    let directory = tempfile::tempdir().unwrap();
    let private = directory.path().join("private");
    std::fs::create_dir(&private).unwrap();
    std::fs::write(private.join("active.log"), b"first line\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let detector = Detector::open(
        store.clone(),
        ManagedWatch {
            definition: ConsumeWatch {
                id: "tail-fence".into(),
                root: private,
                producer_contract: crate::ProducerContract::ActiveFilesRetain,
                source: "tail-fence".into(),
                format: "log".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 2,
                publish_hold: false,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Private,
    )
    .unwrap();
    (directory, store, detector)
}

/// Value (#445): a refused catalog write that is not `STORAGE_PRESSURE` ended the worker and
/// with it the daemon, and only a restart cleared it. Protects, through the real worker loop on a
/// real store: a store that refuses the first catalog write with `STORAGE_FAILURE` leaves the
/// worker running and the watch reported; the write is retried on the backoff and stored once
/// the store accepts it, and a command sent while the failure stands is answered, not dropped.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_refused_catalog_write_is_retried_and_does_not_end_the_worker() {
    let (worker, run) = worker(
        crate::ProducerContract::FinishedFiles,
        &[],
        Arc::new(tokio::sync::Semaphore::new(2)),
    );
    worker
        .store
        .space_floor()
        .refuse_next_write_for_tests("STORAGE_FAILURE");
    tokio::task::LocalSet::new()
        .run_until(async {
            let task = tokio::task::spawn_local(run);
            tokio::time::sleep(SETTLED).await;
            assert!(!task.is_finished(), "the worker ended on a refused write");
            assert!(worker.store.watch_registry().unwrap().is_none());
            // The write is retried once the first backoff has passed, and then stored.
            tokio::time::sleep(RETRY_FIRST + SETTLED).await;
            assert!(worker.store.watch_registry().unwrap().is_some());
            worker.stop.send_replace(true);
            task.await.unwrap().unwrap();
        })
        .await;
}

/// Value (#445): any pass error that names no claim or watch ended the worker (a latched or
/// poisoned store returned `STORAGE_FAILURE` from the pass). Driven here by a closed operation
/// budget, which fails the tail pass the same way. Protects: the worker outlives it, every watch
/// reports the worker's failure in status so an operator sees it, a waiting command is
/// answered with the store's refusal instead of hanging, and a stop still ends the worker
/// cleanly.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_failed_pass_keeps_the_worker_alive_reports_it_and_answers_commands() {
    let operations = Arc::new(tokio::sync::Semaphore::new(2));
    let (worker, run) = worker(
        crate::ProducerContract::ActiveFilesRetain,
        &[("active.log", b"line\n")],
        operations.clone(),
    );
    tokio::task::LocalSet::new()
        .run_until(async {
            let task = tokio::task::spawn_local(run);
            tokio::time::sleep(SETTLED).await;
            operations.close();
            // New bytes wake the tail pass, which then finds its budget closed.
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(worker._directory.path().join("producer/active.log"))
                .unwrap();
            std::io::Write::write_all(&mut file, b"more\n").unwrap();
            tokio::time::sleep(RETRY_FIRST + SETTLED).await;
            assert!(!task.is_finished(), "the worker ended on a store failure");
            let status = worker.registry.snapshot().unwrap();
            let failure = status[0].failure.as_deref().unwrap_or_default();
            assert!(failure.starts_with("worker: "), "{failure}");
            let refused = worker
                .controller
                .retire_claim(watch_admin::ClaimRetirement {
                    watch: "wake".into(),
                    claim: "absent".into(),
                    caller: "test".into(),
                    reason: "failing".into(),
                })
                .await
                .unwrap_err();
            assert_eq!(refused.code, "STORAGE_FAILURE");
            worker.stop.send_replace(true);
            task.await.unwrap().unwrap();
        })
        .await;
}

/// Value (#445): a root renamed away and replaced ended the worker, and the daemon could not
/// start again until a person restored the root. Protects the watch's own recovery: a failed
/// root check parks that watch with the reason in its status, a replacement directory is never
/// adopted however long the retries run, and the exact root restored resumes the watch with its
/// failure cleared.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_replaced_root_parks_its_watch_and_the_exact_root_resumes_it() {
    let (_directory, _store, mut detector) = held_fixture("parked");
    let root = detector.config.root.clone();
    assert!(detector.attempt(Detector::ready).is_some());
    let moved = root.with_file_name("moved-aside");
    std::fs::rename(&root, &moved).unwrap();
    std::fs::create_dir(&root).unwrap();
    assert!(detector.attempt(Detector::ready).is_none());
    assert!(!detector.live());
    assert!(
        detector
            .status_failure()
            .unwrap()
            .contains("configured consume root changed"),
    );
    // The replacement is refused at each retry; the watch stays parked.
    for _ in 0..3 {
        tokio::time::advance(STORE_RETRY_CEILING).await;
        assert!(!detector.resume());
        assert!(!detector.live());
    }
    std::fs::remove_dir(&root).unwrap();
    std::fs::rename(&moved, &root).unwrap();
    tokio::time::advance(STORE_RETRY_CEILING).await;
    assert!(detector.resume());
    assert!(detector.live());
    assert!(detector.status_failure().is_none());
}

/// Value (#445): one watch root missing at start refused the daemon's start (and every later
/// start). Protects: opening a roster whose one root is missing yields the other watch open
/// and this one reported with its reason; the exact root restored opens it on the retry.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_root_missing_at_start_leaves_the_other_watches_open_and_resumes_when_restored() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let definitions = ["present", "absent"]
        .map(|id| {
            let root = directory.path().join(id);
            std::fs::create_dir(&root).unwrap();
            ConsumeWatch {
                id: id.into(),
                root,
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: id.into(),
                format: "log".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 2,
                publish_hold: false,
            }
        })
        .to_vec();
    let catalog = Catalog::load(&store, &definitions).unwrap();
    let missing = definitions[1].root.clone();
    let parked = missing.with_file_name("parked-aside");
    std::fs::rename(&missing, &parked).unwrap();
    let (detectors, unopened) = open_detectors(&store, &catalog, RootBinding::Stored);
    assert_eq!(detectors.len(), 1);
    assert_eq!(unopened.len(), 1);
    let registry = WatchRegistry::default();
    registry.update(&detectors, &unopened, &[]).unwrap();
    let status = registry.snapshot().unwrap();
    assert_eq!(status.len(), 2);
    assert!(status[1].failure.is_some(), "the missing root is reported");
    let mut roster = Roster {
        detectors,
        unopened,
    };
    roster.reopen(&store, RootBinding::Stored);
    assert_eq!(roster.detectors.len(), 1, "the retry is not yet due");
    std::fs::rename(&parked, &missing).unwrap();
    tokio::time::advance(STORE_RETRY_CEILING).await;
    roster.reopen(&store, RootBinding::Stored);
    assert_eq!(roster.detectors.len(), 2);
    assert!(roster.unopened.is_empty());
}

/// A finished-file detector under a fresh store, for a root that tests rename and replace.
fn held_fixture(id: &str) -> (tempfile::TempDir, Store, Detector) {
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let detector = held_detector(store.clone(), &producer, id, false);
    (directory, store, detector)
}

/// A finished-file watch `id` under `producer`, held when `hold` is set, and its detector.
fn held_detector(store: Store, producer: &Path, id: &str, hold: bool) -> Detector {
    Detector::open(
        store,
        ManagedWatch {
            definition: ConsumeWatch {
                id: id.into(),
                root: producer.to_path_buf(),
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: id.into(),
                format: "opaque".into(),
                quiet_ms: 30_000,
                max_directories: 2,
                max_candidates: 4,
                publish_hold: hold,
            },
            revision: 1,
            enabled: true,
        },
        crate::consumption::RootBinding::Stored,
    )
    .unwrap()
}

/// Reserve two finished files of the held watch, as the worker's claim pass does, and return
/// their claim ids.
fn reserve_two_claims(detector: &mut Detector, producer: &Path) -> Vec<String> {
    ["first.bin", "second.bin"]
        .map(|name| {
            let path = producer.join(name);
            std::fs::write(&path, name.as_bytes()).unwrap();
            let identity = CompletedFileIdentity::capture(&path.metadata().unwrap());
            let journal = detector
                .owner
                .claim()
                .unwrap()
                .reserve(Path::new(name), identity, &detector.config.source, "opaque")
                .unwrap();
            journal.import_id
        })
        .to_vec()
}

#[test]
fn a_claim_made_under_a_publish_hold_stays_journaled_and_publishes_when_the_hold_clears() {
    // INVARIANT (issue 280, publish hold): the hold never drops or refuses a claim. A claim
    // reserved while the hold is set stays journaled and in flight, is never picked for
    // publication, and is picked, in pending order, once the hold clears.
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = held_detector(store.clone(), &producer, "held", true);
    let claims = reserve_two_claims(&mut detector, &producer);
    // The worker's pass moves each held claim in flight (`move_held_claim`) before any publish pick.
    for claim in &claims {
        detector.move_held_claim(&store, claim).unwrap();
    }

    let pending = store.consumption_pending().unwrap();
    let mut pending_ids = pending
        .iter()
        .map(|journal| journal.import_id.clone())
        .collect::<Vec<_>>();
    pending_ids.sort();
    let mut expected = claims.clone();
    expected.sort();
    assert_eq!(pending_ids, expected);
    for claim in &claims {
        assert!(producer.join(".sparrow-inflight").join(claim).exists());
    }
    assert_eq!(
        publishable(&pending, std::slice::from_ref(&detector), |_| true).count(),
        0
    );

    // The hold clears: both claims are publishable, in pending order.
    detector.config.publish_hold = false;
    let order = publishable(&pending, std::slice::from_ref(&detector), |_| true)
        .map(|journal| journal.import_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        order,
        pending
            .iter()
            .map(|j| j.import_id.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(order.len(), 2);
}

#[test]
fn a_publish_hold_is_named_by_status_with_its_count_and_since_when() {
    // INVARIANT (issue 280, publish hold): status names the held watch, how many of its
    // pending claims the hold holds, and the oldest held claim first-seen time (not the time the hold began).
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = held_detector(store.clone(), &producer, "held-status", true);
    reserve_two_claims(&mut detector, &producer);
    let pending = store.consumption_pending().unwrap();
    let since = pending
        .iter()
        .filter_map(|journal| journal.history.first_seen)
        .min();

    let registry = WatchRegistry::default();
    registry
        .update(std::slice::from_ref(&detector), &[], &pending)
        .unwrap();
    let status = registry.snapshot().unwrap();
    assert_eq!(status.len(), 1);
    assert!(status[0].definition.publish_hold);
    assert_eq!(status[0].held_claims, 2);
    assert_eq!(status[0].oldest_held_first_seen, since);
    assert!(since.is_some());

    // Clearing the hold names nothing: the held count returns to zero.
    detector.config.publish_hold = false;
    registry
        .update(std::slice::from_ref(&detector), &[], &pending)
        .unwrap();
    let status = registry.snapshot().unwrap();
    assert!(!status[0].definition.publish_hold);
    assert_eq!(
        (status[0].held_claims, status[0].oldest_held_first_seen),
        (0, None)
    );
}

#[test]
fn the_publish_hold_warning_is_written_by_the_update_that_decides_it_once_per_hold() {
    // INVARIANT (issue 280, publish hold): a hold is warned about by the same update that
    // writes the registry, once per daemon start and again only after it clears and is set.
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = held_detector(store.clone(), &producer, "held-start", true);
    reserve_two_claims(&mut detector, &producer);
    let pending = store.consumption_pending().unwrap();

    // A fresh registry is a daemon start: the hold is named once, with its count.
    let registry = WatchRegistry::default();
    let written = registry
        .update(std::slice::from_ref(&detector), &[], &pending)
        .unwrap();
    assert_eq!(written.len(), 1);
    assert!(written[0].contains("watch=held-start") && written[0].contains("2 pending claims"));
    assert!(
        registry
            .update(std::slice::from_ref(&detector), &[], &pending)
            .unwrap()
            .is_empty()
    );

    // Cleared, then set again: the warning is written again.
    detector.config.publish_hold = false;
    assert!(
        registry
            .update(std::slice::from_ref(&detector), &[], &pending)
            .unwrap()
            .is_empty()
    );
    detector.config.publish_hold = true;
    assert_eq!(
        registry
            .update(std::slice::from_ref(&detector), &[], &pending)
            .unwrap()
            .len(),
        1
    );

    // The next daemon start warns again, because nothing was announced before it.
    assert_eq!(
        WatchRegistry::default()
            .update(std::slice::from_ref(&detector), &[], &pending)
            .unwrap()
            .len(),
        1
    );
}

/// Reserve file `name` (written here) under `producer` for `detector`, as the claim pass does.
fn reserve_file(detector: &mut Detector, producer: &Path, name: &str) -> String {
    let path = producer.join(name);
    std::fs::write(&path, name.as_bytes()).unwrap();
    let identity = CompletedFileIdentity::capture(&path.metadata().unwrap());
    detector
        .owner
        .claim()
        .unwrap()
        .reserve(Path::new(name), identity, &detector.config.source, "opaque")
        .unwrap()
        .import_id
}

/// Reserve files for `detector` the way the worker's claim pass does for one watch: only while
/// `held_may_claim` allows (for a held watch), from `files` ready files, in order. Returns how
/// many it claimed.
fn claim_while_allowed(
    store: &Store,
    detector: &mut Detector,
    producer: &Path,
    files: u32,
) -> usize {
    let mut claimed = 0;
    for index in 0..files {
        let pending = store.consumption_pending().unwrap();
        if detector.config.publish_hold && !held_may_claim(&detector.config.id, &pending) {
            break;
        }
        reserve_file(detector, producer, &format!("file-{index}.bin"));
        claimed += 1;
    }
    claimed
}

#[test]
fn a_held_watch_takes_only_its_share_and_other_watches_and_tails_still_claim() {
    // INVARIANT (issue 280, publish hold): a hold on one watch never stops another watch or a
    // tail generation. A held watch claims at most `HELD_CLAIMS_PER_WATCH` files and, with
    // every held watch together, leaves `HELD_RESERVED_SLOTS` journal slots free; its further
    // files stay in the producer's directory. Another watch's file is claimed and is the one
    // publishable claim, and a tail can still begin a generation while the holds are set.
    let (directory, store, mut tail) = tail_detector();
    let mut taken = Vec::new();
    let mut detectors = Vec::new();
    for index in 0..4 {
        let producer = directory.path().join(format!("held-{index}"));
        std::fs::create_dir(&producer).unwrap();
        let mut held = held_detector(store.clone(), &producer, &format!("held-{index}"), true);
        taken.push(claim_while_allowed(&store, &mut held, &producer, 10));
        detectors.push(held);
    }
    // 4 and 4 and 4, then the reserve keeps four slots free for everyone else.
    assert_eq!(
        taken,
        vec![
            HELD_CLAIMS_PER_WATCH,
            HELD_CLAIMS_PER_WATCH,
            HELD_CLAIMS_PER_WATCH,
            0
        ]
    );
    let pending = store.consumption_pending().unwrap();
    assert_eq!(
        pending.len(),
        sparrow_core::MAX_CONSUMPTION_JOURNALS - HELD_RESERVED_SLOTS
    );

    // A second, unheld watch still claims, and its claim is the only one the worker publishes.
    let open = directory.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let mut unheld = held_detector(store.clone(), &open, "open", false);
    let id = reserve_file(&mut unheld, &open, "ready.bin");
    let pending = store.consumption_pending().unwrap();
    detectors.push(unheld);
    let publishable = publishable(&pending, &detectors, |_| true)
        .map(|journal| journal.import_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(publishable, vec![id]);

    // A tail still begins its generation while the holds are set.
    tail.scan_step().unwrap();
    tail.drain().unwrap();
    assert!(tail.failure.is_none(), "{:?}", tail.failure);
    assert_eq!(store.tail_generations("tail-fence").unwrap().len(), 1);
}

#[test]
fn a_held_claim_that_cannot_move_records_one_attempt_until_its_failure_changes() {
    // INVARIANT (issue 280, publish hold): a failed move of a held claim is recorded in the
    // journal and logged once per change of its text. The worker retries the move on every
    // pass, so a permanent failure must not rewrite the journal each time.
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let mut detector = held_detector(store.clone(), &producer, "held-missing", true);
    let id = reserve_file(&mut detector, &producer, "gone.bin");
    std::fs::remove_file(producer.join("gone.bin")).unwrap();
    for _ in 0..5 {
        detector.move_held_claim(&store, &id).unwrap();
    }
    assert_eq!(store.consumption_get(&id).unwrap().history.attempts, 1);
    assert!(detector.status_failure().is_some());
}

/// INVARIANT: a reboot that renumbers a device number never stops the daemon or gaps a
/// checkpoint: an active generation whose stored root and file device differ from the ones
/// now (and whose inodes match) stays active with its records untouched and is resumed by
/// attaching the same file, while a changed root inode still refuses to open.
#[test]
fn tail_open_ignores_a_renumbered_device_and_refuses_a_changed_inode() {
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let (first, second) = (root.join("first.log"), root.join("second.log"));
    std::fs::write(&first, b"first line\n").unwrap();
    std::fs::write(&second, b"second line\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let (root_meta, first_meta, second_meta) = (
        std::fs::metadata(&root).unwrap(),
        std::fs::metadata(&first).unwrap(),
        std::fs::metadata(&second).unwrap(),
    );
    // The stored root and file device numbers are the current ones shifted by `shift`.
    let declare = |request: &str, file: &std::fs::Metadata, shift: u64, inode: u64| {
        let device = root_meta.dev() + shift;
        store
            .tail_start(
                &sparrow_core::TailStart {
                    request_id: request.into(),
                    watch: "tail-boot".into(),
                    watch_revision: 1,
                    directories: sparrow_core::ConsumptionDirectories {
                        root_device: device,
                        root_inode: inode,
                        parent_device: device,
                        parent_inode: inode,
                    },
                    path: b"file.log".to_vec(),
                    source: "tail-boot".into(),
                    format: "log".into(),
                    device: file.dev() + shift,
                    inode: file.ino(),
                },
                crate::now().unwrap(),
            )
            .unwrap()
    };
    let config = ConsumeWatch {
        id: "tail-boot".into(),
        root: root.clone(),
        producer_contract: crate::ProducerContract::ActiveFilesRetain,
        source: "tail-boot".into(),
        format: "log".into(),
        quiet_ms: 30_000,
        max_directories: 2,
        max_candidates: 2,
        publish_hold: false,
    };
    let open = || {
        crate::tailing::TailRoot::open(
            store.clone(),
            &config,
            1,
            crate::consumption::RootBinding::Stored,
        )
    };

    // Inode changed together with a stale device: refused, and nothing is gapped.
    let changed = declare("changed-inode", &first_meta, 1, root_meta.ino() + 1);
    assert!(matches!(open(), Err(crate::consumption::Error::State(_))));
    assert_eq!(store.tail_generation(&changed.id).unwrap().gap, None);
    store
        .tail_discontinue(
            &changed.id,
            changed.offset,
            "read_gap",
            crate::now().unwrap(),
        )
        .unwrap();

    // Device renumbered alone, root and file: the open succeeds, the generation stays active
    // with its checkpoint, and attaching the same file resumes it instead of starting over.
    let mut renumbered = declare("renumbered", &second_meta, 1, root_meta.ino());
    store
        .tail_append(
            &renumbered.id,
            0,
            b"second line\n",
            false,
            crate::now().unwrap(),
        )
        .unwrap();
    renumbered = store.tail_generation(&renumbered.id).unwrap();
    assert_eq!(renumbered.records, 1);
    let mut tail = open().unwrap();
    let after = store.tail_generation(&renumbered.id).unwrap();
    assert_eq!(after.gap, None);
    assert_eq!(after.offset, renumbered.offset);
    assert_eq!(after.records, 1);
    tail.attach(std::path::Path::new("second.log"), &config, 1)
        .unwrap();
    let (generations, _, held, _) = tail.status("tail-boot").unwrap();
    assert_eq!(held, 1);
    assert_eq!(
        generations.len(),
        2,
        "attaching resumed the declared generation instead of starting a second one"
    );
}

/// INVARIANT: a pending finished-file claim whose stored root, parent and file device numbers
/// all differ from the ones now (a reboot renumbers them) stays bound: the watch root opens
/// and recovery passes the scope checks and the file identity check, because only the inode
/// binds, and a handoff intent saved under the old device is admitted and retried as the
/// same journal. A claim stored under another root inode is still refused.
#[test]
fn consume_root_binds_by_inode_across_a_renumbered_device_and_refuses_a_changed_inode() {
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir().unwrap();
    let producer = directory.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let path = producer.join("done.log");
    std::fs::write(&path, b"line\n").unwrap();
    let data = directory.path().join("store");
    let config_path =
        sparrow_test_support::write_server_config(directory.path(), &data, &data.join("rpc.sock"));
    let store = crate::ServerConfig::read(&config_path)
        .unwrap()
        .open_store()
        .unwrap();
    let (root, file) = (
        std::fs::metadata(&producer).unwrap(),
        std::fs::metadata(&path).unwrap(),
    );
    let reserve = |watch: &str, device: u64, inode: u64| {
        store
            .consumption_reserve(
                watch,
                b"done.log",
                "consume-boot",
                "log",
                {
                    // The store requires the claimed file and the roots on one device.
                    let mut observed = CompletedFileIdentity::capture(&file);
                    observed.device = device;
                    crate::consumption::generation(observed).unwrap()
                },
                sparrow_core::ConsumptionDirectories {
                    root_device: device,
                    root_inode: inode,
                    parent_device: device,
                    parent_inode: inode,
                },
                crate::now().unwrap(),
            )
            .unwrap()
    };

    // Inode changed (with a stale device too): the open refuses.
    reserve("changed-inode", root.dev() + 1, root.ino() + 1);
    assert!(matches!(
        ClaimRoot::open(store.clone(), "changed-inode", &producer),
        Err(crate::consumption::Error::State(_))
    ));

    // Device renumbered alone: the open succeeds and recovery is not refused by the root,
    // parent or file identity checks; it moves the source into the private claim directory.
    let journal = reserve("renumbered", root.dev() + 1, root.ino());
    let owner = ClaimRoot::open(store.clone(), "renumbered", &producer).unwrap();
    let recovered = owner.recover(&journal.import_id, crate::now().unwrap());
    assert!(
        matches!(recovered, Ok(crate::consumption::Recovery::Claimed(_))),
        "the renumbered root and file are bound by inode: {:?}",
        recovered.err()
    );
    assert!(
        !path.exists(),
        "the claimed source moved into the private claim directory"
    );

    // A handoff intent saved before the reboot carries the old file device. Its first
    // admission is accepted against the live root, and a retry observed under the current
    // device returns the same journal.
    store.watch_registry_replace(None, b"registered").unwrap();
    let handed = producer.join("handed.log");
    std::fs::write(&handed, b"handed\n").unwrap();
    let mut stale = CompletedFileIdentity::capture(&std::fs::metadata(&handed).unwrap());
    stale.device += 1;
    let intent = |observed: CompletedFileIdentity| sparrow_core::ConsumptionHandoff {
        request_id: "handoff-renumbered".into(),
        watch: "renumbered".into(),
        watch_revision: 1,
        relative_path: b"handed.log".to_vec(),
        observed: crate::consumption::generation(observed).unwrap(),
        tail_generation_id: None,
    };
    let admitted = owner
        .handoff(&intent(stale), "consume-boot", "log", crate::now().unwrap())
        .unwrap();
    let current = CompletedFileIdentity::capture(&std::fs::metadata(&handed).unwrap());
    let retried = owner
        .handoff(
            &intent(current),
            "consume-boot",
            "log",
            crate::now().unwrap(),
        )
        .unwrap();
    assert_eq!(retried.import_id, admitted.import_id);
}
