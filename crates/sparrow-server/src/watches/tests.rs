// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

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
    )
    .unwrap();
    detector.scan_step().unwrap();
    detector.drain().unwrap();
    assert!(detector.covered);
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
