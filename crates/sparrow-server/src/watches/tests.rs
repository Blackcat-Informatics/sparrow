// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Real owned-kernel queues exercise the production detector's coverage epoch.

use super::*;

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
        .update(std::slice::from_ref(&detector), &[])
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
    detectors[0].failure = Some(format!("claim {cause}: missing"));
    detectors[0].failed_claim = Some(cause.clone());
    retire(&mut detectors, &other);
    assert_eq!(
        detectors[0].failure.as_deref(),
        Some(format!("claim {cause}: missing").as_str())
    );
    assert_eq!(detectors[0].failed_claim.as_deref(), Some(cause.as_str()));

    // Retiring the claim that set the failure clears it.
    retire(&mut detectors, &cause);
    assert_eq!(detectors[0].failure, None);
    assert_eq!(detectors[0].failed_claim, None);

    // A discovery failure names no claim, so no retirement clears it.
    detectors[0].failure = Some("discovery refused".into());
    retire(&mut detectors, &discovered);
    assert_eq!(detectors[0].failure.as_deref(), Some("discovery refused"));
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
/// disposable copy never rewrites where its watches consume.
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
