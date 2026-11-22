// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! A real full filesystem under the store (#447). The test needs real time (RocksDB polls free
//! space every 5 s to resume a handle a full disk stopped) and a filesystem it can fill, so it
//! is ignored in the gate. Run it with `tools/enospc-check.sh LOG core A` or `... core B`: the
//! script mounts a 160 MiB tmpfs in a user and mount namespace (`unshare -rm`, no root) and
//! names it in `SPARROW_ENOSPC_DIR`. Arm `A` repeats the proof of durability every second after
//! space returns. Arm `B` leaves the store alone and proves once at the end.
//! Arm `A0-reopen` reproduces the wedge and replaces the stopped handle.

use sparrow_core::{Config, Durability, DurabilityWatermark, Store};
use std::{io::Write, path::Path, time::Duration};

/// How long the wait after space returns lasts: twenty-four of RocksDB's 5 s polls.
const WAIT: Duration = Duration::from_secs(120);

fn config() -> Config {
    Config {
        // Large enough that Sparrow's own budget never refuses before the filesystem does.
        disk_budget: 4 * 1024 * 1024 * 1024,
        maintenance_reserve: 1024 * 1024,
        min_free_bytes: 4 * 1024,
        ..sparrow_test_support::storage_config!(Config)
    }
}

fn resumed(data: &Path) -> bool {
    std::fs::read_to_string(data.join("LOG"))
        .is_ok_and(|log| log.contains("Successfully resumed DB"))
}

/// INVARIANT (#447): a WAL write that fails with no space left latches durability uncertain
/// with a retryable refusal and leaves reads served; once space returns RocksDB resumes the
/// handle by itself when the store is left alone, the proof of durability then succeeds and
/// the latch clears in the running store. Arm `A` is the control that repeats the proof through
/// the wait and reports its result. Arm `A0-reopen` requires a native wedge and verifies
/// that closing and opening the original store restores reads and retries.
#[test]
#[ignore = "runs under tools/enospc-check.sh on a 160 MiB tmpfs and needs about four minutes"]
fn a_full_disk_latch_clears_once_space_returns() {
    let dir = std::path::PathBuf::from(
        std::env::var_os("SPARROW_ENOSPC_DIR")
            .expect("SPARROW_ENOSPC_DIR names the small filesystem; run tools/enospc-check.sh"),
    );
    let arm = std::env::var("SPARROW_ENOSPC_ARM").unwrap_or_else(|_| "B".to_owned());
    let data = dir.join("store");
    let store = Store::open(&data, config()).unwrap();
    let before = store
        .ingest("before", "source", "log", b"kept\n", 1)
        .unwrap();
    let before_status = store.durability_status().unwrap();
    let before_fence = DurabilityWatermark {
        store_id: store.store_id().unwrap(),
        session_id: before_status.session_id,
        sequence: before_status.durable_through,
    };

    // Fill the filesystem, leaving 16 KiB so the floor admits a write and the WAL append is
    // what runs out of space. The WAL file was preallocated before the fill, so appends fit in
    // it until the memtable fills; the writes below run past that point.
    let filler = dir.join("filler");
    let mut file = std::fs::File::create(&filler).unwrap();
    let chunk = vec![0_u8; 64 * 1024];
    while file.write_all(&chunk).is_ok() {}
    drop(file);
    let length = std::fs::metadata(&filler).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&filler)
        .unwrap()
        .set_len(length - 16 * 1024)
        .unwrap();
    let df = |label: &str| {
        let out = std::process::Command::new("df")
            .arg("-B1")
            .arg(&dir)
            .output()
            .unwrap();
        eprintln!("{label}: {}", String::from_utf8_lossy(&out.stdout));
    };
    df("after fill");
    // Incompressible and different every time: the store keeps identical chunks once.
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut payload = |size: usize| -> Vec<u8> {
        (0..size)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed.to_le_bytes()[0]
            })
            .collect()
    };
    let failed = (0..480)
        .find_map(|attempt| {
            store
                .ingest_with_durability(
                    &format!("full-{attempt}"),
                    "source",
                    "opaque",
                    &payload(256 * 1024),
                    2,
                    Durability::Durable,
                )
                .err()
        })
        .expect("a write met no space");
    eprintln!("first failure: {failed:?}");
    let latched = store.durability_status().unwrap();
    assert!(latched.failure.is_some(), "{latched:?}");
    let refused = store
        .ingest("refused", "source", "log", b"refused\n", 3)
        .unwrap_err();
    assert!(refused.is_retryable(), "{refused:?}");
    assert!(
        refused.message.contains("durability uncertain"),
        "{refused:?}"
    );
    store
        .artifact(&before.id)
        .expect("reads stay served while latched");

    std::fs::remove_file(&filler).unwrap();
    df("space returned");
    let began = std::time::Instant::now();
    let mut resumed_after = None;
    let mut cleared_after = None;
    // Arms `S0` and `S5` make a single proof as space returns (at 0 s and at 0.5 s) and then
    // leave the store alone like arm `B`.
    if arm == "S5" {
        std::thread::sleep(Duration::from_millis(500));
    }
    if arm.starts_with('S') {
        eprintln!(
            "the single early proof: {:?}",
            store.reestablish_durability()
        );
    }
    // Arm `A0` starts at the moment space returns and repeats every 0.5 s.
    let eager = arm.starts_with("A0");
    let step = Duration::from_millis(if eager { 500 } else { 1000 });
    while began.elapsed() < WAIT && cleared_after.is_none() {
        if !eager {
            std::thread::sleep(step);
        }
        if resumed_after.is_none() && resumed(&data) {
            resumed_after = Some(began.elapsed());
        }
        // Arms A and A0 are the old retry: a proof, which flushes every column family.
        if arm.starts_with('A') {
            let proof = store.reestablish_durability();
            if resumed_after.is_none() && resumed(&data) {
                resumed_after = Some(began.elapsed());
            }
            if proof.is_ok() {
                cleared_after = Some(began.elapsed());
            }
        }
        if eager {
            std::thread::sleep(step);
        }
    }
    if !arm.starts_with('A') {
        let proof = store.reestablish_durability();
        eprintln!("the one proof: {proof:?}");
        if proof.is_ok() {
            cleared_after = Some(began.elapsed());
        }
    }
    eprintln!("RESULT arm={arm} resumed_after={resumed_after:?} cleared_after={cleared_after:?}");
    if arm == "A0-reopen" {
        assert!(
            resumed_after.is_none() && cleared_after.is_none(),
            "the native wedge did not reproduce"
        );
        assert!(store.durability_status().unwrap().failure.is_some());
        assert!(store.close_for_reopen().is_empty());
        let reopened = Store::reopen(&data, config(), &before_fence.store_id).unwrap();
        let status = reopened.durability_status().unwrap();
        assert_ne!(status.session_id, before_fence.session_id);
        assert!(status.failure.is_none());
        assert_eq!(
            reopened.flush_through(&before_fence).unwrap_err().code,
            "STALE_WATERMARK"
        );
        assert_eq!(reopened.bytes(&before.id, 0, 5, 4).unwrap(), b"kept\n");
        let retried = reopened
            .ingest("refused", "source", "log", b"refused\n", 3)
            .unwrap();
        assert!(retried.buffered.is_none());
        eprintln!(
            "RESULT pid={} store={} new_session={}",
            std::process::id(),
            before_fence.store_id,
            status.session_id
        );
    } else if arm == "B" {
        assert!(cleared_after.is_some(), "the latch did not clear");
        let after = store
            .ingest("after", "source", "log", b"after\n", 4)
            .unwrap();
        drop(store);
        let store = Store::open(&data, config()).unwrap();
        assert!(store.artifact(&before.id).is_ok());
        assert!(store.artifact(&after.id).is_ok());
    }
}
