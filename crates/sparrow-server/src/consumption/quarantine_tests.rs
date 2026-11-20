// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Permanent refusals leave the retry loop: the source is quarantined, never lost.

use super::{ClaimRoot, Error, Recovery, Refusal};
use crate::ServerConfig;
use sparrow_client::CompletedFileIdentity;
use std::{os::unix::fs::PermissionsExt, path::Path};

fn domain(code: &str, retryable: bool) -> Error {
    Error::Upload(sparrow_client::Error::Domain {
        code: code.into(),
        message: "refused".into(),
        retryable,
        class: sparrow_client::FailureClass::Unclassified,
    })
}

/// INVARIANT (#417): only the daemon's non-retryable refusal of the input is permanent. A
/// retryable refusal, a transport or local failure and a daemon fault keep retrying.
#[test]
fn only_a_non_retryable_input_refusal_is_permanent() {
    let cases = [
        (domain("RESOURCE_LIMIT", false), true),
        (domain("INVALID_ARGUMENT", false), true),
        (domain("RESOURCE_LIMIT", true), false),
        (domain("STORAGE_PRESSURE", true), false),
        (domain("STORAGE_FAILURE", false), false),
        (
            Error::Upload(sparrow_client::Error::Domain {
                code: "RESOURCE_LIMIT".into(),
                message: "the claim is pending".into(),
                retryable: false,
                class: sparrow_client::FailureClass::Local,
            }),
            false,
        ),
        (Error::State("consume publication deadline expired"), false),
        (
            Error::Storage(sparrow_core::Error::new("RESOURCE_LIMIT", "advertise")),
            false,
        ),
    ];
    for (error, permanent) in cases {
        assert_eq!(Refusal::of(&error).is_some(), permanent, "{error}");
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    store: sparrow_core::Store,
    producer: std::path::PathBuf,
    root: ClaimRoot,
    id: String,
}

/// A watch root holding one claimed source that the daemon refused.
fn claimed() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let producer = dir.path().join("intake").join("watch");
    std::fs::create_dir_all(&producer).unwrap();
    let data = dir.path().join("store");
    let config =
        sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
    let store = ServerConfig::read(&config).unwrap().open_store().unwrap();
    let path = producer.join("big.json");
    std::fs::write(&path, b"{}\n").unwrap();
    let identity = CompletedFileIdentity::capture(&path.metadata().unwrap());
    let root = ClaimRoot::open(store.clone(), "refused", &producer).unwrap();
    let journal = root
        .reserve(Path::new("big.json"), identity, "refused-source", "json")
        .unwrap();
    let now = crate::now().unwrap();
    assert!(matches!(
        root.recover(&journal.import_id, now).unwrap(),
        Recovery::Claimed(_)
    ));
    Fixture {
        _dir: dir,
        store,
        producer,
        root,
        id: journal.import_id,
    }
}

fn refusal() -> Refusal {
    Refusal::of(&domain("RESOURCE_LIMIT", false)).unwrap()
}

/// INVARIANT (#417): a refused source is renamed into a sibling quarantine directory with its
/// refusal beside it and the claim is retired with the refusal as its reason, freeing the
/// journal slot. The source is never copied or deleted.
#[test]
fn a_refused_source_is_quarantined_and_its_claim_retired_with_the_refusal() {
    let f = claimed();
    let now = crate::now().unwrap();
    let retired = f
        .root
        .quarantine(&f.producer, &[], &f.id, &refusal(), now)
        .unwrap();
    let quarantine = f
        .producer
        .parent()
        .unwrap()
        .join("watch.sparrow-quarantine");
    assert_eq!(std::fs::read(quarantine.join(&f.id)).unwrap(), b"{}\n");
    let note = std::fs::read_to_string(quarantine.join(format!("{}.refusal", f.id))).unwrap();
    assert!(note.starts_with("code=RESOURCE_LIMIT\nmessage=refused\npath=big.json\n"));
    assert!(!f.producer.join(".sparrow-inflight").join(&f.id).exists());
    assert!(
        retired
            .retirement
            .reason
            .starts_with("non-retryable RESOURCE_LIMIT: refused;")
    );
    assert!(f.store.consumption_pending().unwrap().is_empty());
    assert_eq!(f.store.consumption_retired().unwrap().len(), 1);
}

/// INVARIANT (#417): a stop between the move and the retirement is finished from the refusal
/// on file, with no second copy and no lost source.
#[test]
fn a_move_interrupted_before_the_retirement_is_finished_from_its_refusal_file() {
    let f = claimed();
    let now = crate::now().unwrap();
    assert_eq!(
        f.root
            .interrupted_quarantine(&f.producer, &[], &f.id)
            .unwrap(),
        None
    );
    let quarantine = f
        .producer
        .parent()
        .unwrap()
        .join("watch.sparrow-quarantine");
    std::fs::create_dir(&quarantine).unwrap();
    std::fs::set_permissions(&quarantine, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        quarantine.join(format!("{}.refusal", f.id)),
        "code=RESOURCE_LIMIT\nmessage=refused\npath=big.json\n",
    )
    .unwrap();
    std::fs::rename(
        f.producer.join(".sparrow-inflight").join(&f.id),
        quarantine.join(&f.id),
    )
    .unwrap();
    let found = f
        .root
        .interrupted_quarantine(&f.producer, &[], &f.id)
        .unwrap()
        .unwrap();
    assert_eq!(found, refusal());
    f.root
        .quarantine(&f.producer, &[], &f.id, &found, now)
        .unwrap();
    assert_eq!(std::fs::read(quarantine.join(&f.id)).unwrap(), b"{}\n");
    assert!(f.store.consumption_pending().unwrap().is_empty());
}

/// INVARIANT (#417): when the quarantine cannot be written the source stays in its private
/// claim directory and the claim stays pending; nothing is lost or retired.
#[test]
fn an_unwritable_quarantine_leaves_the_source_and_the_claim_in_place() {
    let f = claimed();
    let parent = f.producer.parent().unwrap();
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = f
        .root
        .quarantine(&f.producer, &[], &f.id, &refusal(), crate::now().unwrap());
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    assert!(f.producer.join(".sparrow-inflight").join(&f.id).exists());
    assert_eq!(f.store.consumption_pending().unwrap().len(), 1);
    assert!(f.store.consumption_retired().unwrap().is_empty());
}

/// INVARIANT (#417): a source found in both the quarantine and the private claim directory is
/// never retired, and a quarantine that is a configured root is never written; each leaves the
/// claim pending and the source where it was.
#[test]
fn quarantine_refuses_a_duplicated_source_and_a_configured_root() {
    let f = claimed();
    let quarantine = f
        .producer
        .parent()
        .unwrap()
        .join("watch.sparrow-quarantine");
    let now = crate::now().unwrap();
    let as_root = [quarantine.clone()];
    assert!(
        f.root
            .quarantine(&f.producer, &as_root, &f.id, &refusal(), now)
            .is_err()
    );
    assert!(!quarantine.exists());
    std::fs::create_dir(&quarantine).unwrap();
    std::fs::set_permissions(&quarantine, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(quarantine.join(&f.id), b"{}\n").unwrap();
    assert!(
        f.root
            .quarantine(&f.producer, &[], &f.id, &refusal(), now)
            .is_err()
    );
    assert!(f.producer.join(".sparrow-inflight").join(&f.id).exists());
    assert_eq!(f.store.consumption_pending().unwrap().len(), 1);
}

/// INVARIANT (#417): a configured root that reaches the quarantine directory through `..` is
/// found by identity, which the lexical name check cannot see; no source is written into it.
/// A bind-mount alias is checked by the same device and inode comparison and is not tested.
#[test]
fn a_root_reaching_the_quarantine_through_dotdot_is_refused_by_identity() {
    let f = claimed();
    let parent = f.producer.parent().unwrap();
    let quarantine = parent.join("watch.sparrow-quarantine");
    std::fs::create_dir(&quarantine).unwrap();
    std::fs::set_permissions(&quarantine, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alias = parent
        .join("watch")
        .join("..")
        .join("watch.sparrow-quarantine");
    let result = f.root.quarantine(
        &f.producer,
        &[alias],
        &f.id,
        &refusal(),
        crate::now().unwrap(),
    );
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("same directory as a configured consume root"),
        "{error}"
    );
    assert!(!quarantine.join(&f.id).exists());
    assert!(f.producer.join(".sparrow-inflight").join(&f.id).exists());
}

/// INVARIANT (#417): a configured root that does not exist cannot be the quarantine, so it
/// neither parks the move nor hides an interrupted one; other open errors stay fatal.
#[test]
fn a_missing_configured_root_does_not_stop_the_quarantine() {
    let f = claimed();
    let missing = f.producer.parent().unwrap().join("gone");
    let roots = [missing, f.producer.clone()];
    f.root
        .quarantine(
            &f.producer,
            &roots,
            &f.id,
            &refusal(),
            crate::now().unwrap(),
        )
        .unwrap();
    assert!(f.store.consumption_pending().unwrap().is_empty());
}
