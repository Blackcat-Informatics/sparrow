// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Real SIGKILL recovery through the original claim, SDK and storage owners.

use super::{ClaimRoot, Recovery};
use crate::{Rpc, ServerConfig, sparrow_capnp as wire};
use sparrow_client::{Client, CompletedFileIdentity};
use std::{
    fs,
    os::unix::{fs::FileTypeExt, process::ExitStatusExt},
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};

const INPUT: &[u8] = &[b'a'; 80 * 1024];
const REPLACEMENT: &[u8] = b"independent replacement source\n";
const CHILD_CONFIG: &str = "SPARROW_TEST_CRASH_CONFIG";
const CHILD_IMPORT: &str = "SPARROW_TEST_CRASH_IMPORT";
type Snapshot = (String, Vec<String>, u64, u64, String, u64);

fn finish(config_path: &Path, import: &str) -> Snapshot {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let config = ServerConfig::read(config_path).unwrap();
        let store = config.open_store().unwrap();
        let producer = config_path.parent().unwrap().join("producer");
        let original = producer.join("finished.bin");
        let root = ClaimRoot::open(store.clone(), "crash-watch", &producer).unwrap();
        match fs::symlink_metadata(&config.socket_path) {
            Ok(metadata) => {
                assert!(metadata.file_type().is_socket());
                fs::remove_file(&config.socket_path).unwrap();
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("inspect owned test socket: {error}"),
        }
        let listener = tokio::net::UnixListener::bind(&config.socket_path).unwrap();
        let capability: wire::sparrow::Client = capnp_rpc::new_client(Rpc::new(
            store.clone(),
            Arc::new(tokio::sync::Semaphore::new(4)),
            config.max_pin_members,
        ));
        let serving = tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            crate::serve_connection(stream, capability).await.unwrap();
        });
        let client = Client::connect(&config.socket_path).await.unwrap();
        let now = crate::now().unwrap();
        match root.recover(import, now).unwrap() {
            Recovery::Claimed(mut claim) => {
                if original.exists() {
                    assert_eq!(fs::read(&original).unwrap(), REPLACEMENT);
                } else {
                    fs::write(&original, REPLACEMENT).unwrap();
                }
                let receipt = root
                    .publish(&client, &mut claim, &Default::default())
                    .await
                    .unwrap();
                let removed = root.remove(&claim, crate::now().unwrap()).unwrap();
                assert_eq!(
                    removed.published_artifact.as_deref(),
                    Some(receipt.as_str())
                );
            }
            Recovery::Removed(journal) => assert!(journal.removed_at.is_some()),
        }
        let now = crate::now().unwrap();
        let removed = store.consumption_get(import).unwrap();
        assert!(removed.removed_at.is_some());
        let artifact = store.import_publish(import, now).unwrap();
        assert_eq!(
            removed.published_artifact.as_deref(),
            Some(artifact.id.as_str())
        );
        assert_eq!(store.records(now).unwrap().len(), 1);
        assert_eq!(artifact.record_ids.len(), 1);
        assert!(artifact.buffered.is_none());
        assert_eq!(
            store.bytes(&artifact.id, 0, INPUT.len(), now).unwrap(),
            INPUT
        );
        assert_eq!(fs::read(&original).unwrap(), REPLACEMENT);
        assert!(!producer.join(".sparrow-inflight").join(import).exists());
        match root.recover(import, now + 1).unwrap() {
            Recovery::Removed(retry) => assert_eq!(retry, removed),
            Recovery::Claimed(_) => panic!("removed source was reclaimed on retry"),
        }
        let snapshot = (
            artifact.id,
            artifact.record_ids,
            artifact.received_at,
            artifact.expires_at,
            artifact.payload_id,
            removed.removed_at.unwrap(),
        );
        drop(root);
        drop(client);
        serving.await.unwrap();
        store.flush().unwrap();
        snapshot
    }))
}

#[test]
fn whole_process_crashes_preserve_claims_publication_and_replacements() {
    if let Some(config) = std::env::var_os(CHILD_CONFIG) {
        let import = std::env::var(CHILD_IMPORT).expect("child import identity");
        finish(Path::new(&config), &import);
        return;
    }
    for (phase, published) in [
        ("claim_before_rename", false),
        ("claim_after_rename", false),
        ("claim_contents_synced", false),
        ("claim_destination_synced", false),
        ("claim_parent_synced", false),
        ("claim_checkpointed", false),
        ("upload_chunk_committed", false),
        ("claim_published", true),
        ("remove_before_unlink", true),
        ("remove_after_unlink", true),
        ("remove_directory_synced", true),
        ("remove_checkpointed", true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let producer = directory.path().join("producer");
        fs::create_dir(&producer).unwrap();
        let original = producer.join("finished.bin");
        fs::write(&original, INPUT).unwrap();
        let data = directory.path().join("store");
        let config_path = sparrow_test_support::write_server_config(
            directory.path(),
            &data,
            &data.join("rpc.sock"),
        );
        let config = ServerConfig::read(&config_path).unwrap();
        let store = config.open_store().unwrap();
        let root = ClaimRoot::open(store.clone(), "crash-watch", &producer).unwrap();
        let reserved = root
            .reserve(
                Path::new("finished.bin"),
                CompletedFileIdentity::capture(&original.metadata().unwrap()),
                "crash-source",
                "opaque",
            )
            .unwrap();
        drop(root);
        drop(store);
        let ready = directory.path().join("ready");
        let stderr = directory.path().join("child.stderr");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", "consumption::crash_tests::whole_process_crashes_preserve_claims_publication_and_replacements", "--nocapture"])
            .env(CHILD_CONFIG, &config_path).env(CHILD_IMPORT, &reserved.import_id)
            .env("SPARROW_TEST_CRASH_POINT", phase).env("SPARROW_TEST_CRASH_READY", &ready)
            .stdout(Stdio::null()).stderr(fs::File::create_new(&stderr).unwrap());
        let status = sparrow_test_support::crash::kill_at_checkpoint(&mut command, &ready, phase)
            .unwrap_or_else(|error| {
                panic!(
                    "{phase}: {error}; child stderr: {}",
                    fs::read_to_string(&stderr).unwrap()
                )
            });
        assert_eq!(status.signal(), Some(9), "{phase}");
        let store = config.open_store().unwrap();
        let journal = store.consumption_get(&reserved.import_id).unwrap();
        assert_eq!(journal.observed, reserved.observed, "{phase}");
        let uncheckpointed = matches!(
            phase,
            "claim_before_rename"
                | "claim_after_rename"
                | "claim_contents_synced"
                | "claim_destination_synced"
                | "claim_parent_synced"
        );
        assert_eq!(journal.claimed.is_some(), !uncheckpointed, "{phase}");
        assert_eq!(
            journal.removed_at.is_some(),
            phase == "remove_checkpointed",
            "{phase}"
        );
        let private_absent = matches!(
            phase,
            "claim_before_rename"
                | "remove_after_unlink"
                | "remove_directory_synced"
                | "remove_checkpointed"
        );
        assert_eq!(
            producer
                .join(".sparrow-inflight")
                .join(&reserved.import_id)
                .exists(),
            !private_absent,
            "{phase}"
        );
        assert_eq!(journal.published_artifact.is_some(), published, "{phase}");
        assert_eq!(
            store.records(crate::now().unwrap()).unwrap().len(),
            usize::from(published),
            "{phase}"
        );
        if phase == "upload_chunk_committed" {
            let staged = store
                .import_get(&reserved.import_id, crate::now().unwrap())
                .unwrap();
            assert_eq!(
                staged.received_bytes,
                sparrow_core::IMPORT_CHUNK_BYTES as u64
            );
            assert!(staged.received_bytes < INPUT.len() as u64 && !staged.sealed);
        }
        let earlier = journal
            .published_artifact
            .as_ref()
            .map(|id| store.artifact(id).unwrap());
        drop(store);
        if !original.exists() {
            fs::write(&original, REPLACEMENT).unwrap();
        }
        let recovered = finish(&config_path, &reserved.import_id);
        if let Some(artifact) = earlier {
            assert_eq!(recovered.0, artifact.id);
            assert_eq!(recovered.1, artifact.record_ids);
            assert_eq!(recovered.2, artifact.received_at);
            assert_eq!(recovered.3, artifact.expires_at);
            assert_eq!(recovered.4, artifact.payload_id);
        }
        assert_eq!(
            finish(&config_path, &reserved.import_id),
            recovered,
            "retry after reopen: {phase}"
        );
        eprintln!("PASS whole-process crash checkpoint: {phase}");
    }
}
