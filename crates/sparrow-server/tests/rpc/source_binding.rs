// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Source admission through real Unix and TCP producer paths.

use super::{
    begin_import, configure_ingest_socket, configure_tcp_listener, delivery_event, rpc_error_code,
    rpc_ok, start, start_with_ingest, tcp_client, upload_ref, wire, write_server_config,
};
use sparrow_client::{Client, Endpoint, Error, EventBatch};
use std::{fs, path::Path};
use tokio::task::LocalSet;

fn configure(path: &Path, bindings: &str) {
    let original = fs::read_to_string(path).unwrap();
    fs::write(path, format!("{original}\n{bindings}")).unwrap();
}

fn denied<T>(result: Result<T, Error>, source: &str, caller: &str) {
    assert!(matches!(
        result,
        Err(Error::Domain { code, message, retryable: false, .. })
            if code == "PERMISSION_DENIED" && message.contains(source) && message.contains(caller)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn bound_source_refuses_writes_from_other_access_classes() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation, _port_lease) = configure_tcp_listener(&config);
            drop(reservation);
            let uid = rustix::process::geteuid().as_raw();
            configure(
                &config,
                &format!(
                    r#"
[[source_bindings]]
source = "unix."
prefix = true
allow = [{{ class = "private-unix", uid = {uid} }}]

[[source_bindings]]
source = "chap"
allow = [{{ class = "chap" }}]

[[source_bindings]]
source = "guest"
allow = [{{ class = "anonymous-tcp" }}]
"#
                ),
            );
            let _daemon = start(&config, &socket).await;
            let private = Client::connect(&socket).await.unwrap();
            let anonymous = Client::connect_producer(&Endpoint::Tcp(address))
                .await
                .unwrap();
            let authenticated = tcp_client(address).await.unwrap();

            for (caller, source) in [
                (&private, "unix.service"),
                (&authenticated, "chap"),
                (&anonymous, "guest"),
                (&anonymous, "unbound"),
            ] {
                caller
                    .ingest(source, source, wire::Format::Log, b"allowed\n")
                    .await
                    .unwrap();
            }
            for (caller, class, source) in [
                (&anonymous, "anonymous-tcp", "unix.service"),
                (&anonymous, "anonymous-tcp", "chap"),
                (&authenticated, "chap", "guest"),
            ] {
                denied(
                    caller
                        .ingest("different-caller", source, wire::Format::Log, b"refused\n")
                        .await,
                    source,
                    class,
                );
            }

            let mut event = delivery_event("bound event");
            event.sequence = 1;
            let batch = EventBatch {
                request_id: "anonymous-event".into(),
                source: "unix.events".into(),
                client_instance: [5; 16],
                events: vec![event],
            };
            denied(
                anonymous.events(&batch).await,
                "unix.events",
                "anonymous-tcp",
            );
            let mut sealed = anonymous.capability().sealed_events_ingest_request();
            sealed.get().set_store_id(&anonymous.discovery().store_id);
            sealed.get().set_deadline_ms(u64::MAX);
            batch.encode(sealed.get().init_batch()).unwrap();
            let result = sealed.send().promise.await.unwrap();
            assert_eq!(
                rpc_error_code(result.get().unwrap().get_result().unwrap()),
                "PERMISSION_DENIED"
            );
            private.events(&batch).await.unwrap();

            let name = "resume-private-input";
            let id = begin_import(private.capability(), name, "unix.stage")
                .await
                .unwrap();
            let store = &private.discovery().store_id;
            let mut append = anonymous.capability().import_append_request();
            let mut request = append.get().init_request();
            upload_ref(request.reborrow().init_import(), store, &id);
            request.set_bytes(name.as_bytes());
            let result = append.send().promise.await.unwrap();
            assert_eq!(
                rpc_error_code(result.get().unwrap().get_result().unwrap()),
                "PERMISSION_DENIED"
            );

            let mut publish = anonymous.capability().import_publish_request();
            upload_ref(publish.get().init_import(), store, &id);
            let result = publish.send().promise.await.unwrap();
            assert_eq!(
                rpc_error_code(result.get().unwrap().get_result().unwrap()),
                "PERMISSION_DENIED"
            );

            let mut cancel = anonymous.capability().import_cancel_request();
            upload_ref(cancel.get().init_import(), store, &id);
            let result = cancel.send().promise.await.unwrap();
            assert_eq!(
                rpc_error_code(result.get().unwrap().get_result().unwrap()),
                "PERMISSION_DENIED"
            );

            let mut append = private.capability().import_append_request();
            let mut request = append.get().init_request();
            upload_ref(request.reborrow().init_import(), store, &id);
            request.set_bytes(name.as_bytes());
            let result = append.send().promise.await.unwrap();
            assert_eq!(
                rpc_ok(result.get().unwrap().get_result().unwrap()).get_received_bytes(),
                name.len() as u64
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn bound_source_refuses_another_uid() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (ingest, _) = configure_ingest_socket(dir.path(), &config, 4);
            let uid = rustix::process::geteuid().as_raw();
            let wrong = uid ^ 1;
            configure(
                &config,
                &format!(
                    r#"
[[source_bindings]]
source = "wrong-uid"
allow = [
    {{ class = "private-unix", uid = {wrong} }},
    {{ class = "ingest-socket", uid = {wrong} }},
]

[[source_bindings]]
source = "right-uid"
allow = [
    {{ class = "private-unix", uid = {uid} }},
    {{ class = "ingest-socket", uid = {uid} }},
]
"#
                ),
            );
            let _daemon = start_with_ingest(&config, &socket, &ingest).await;
            let private = Client::connect(&socket).await.unwrap();
            let ingest = Client::connect(&ingest).await.unwrap();
            for (client, class) in [(&private, "private-unix"), (&ingest, "ingest-socket")] {
                let denied_id = format!("denied-{class}");
                denied(
                    client
                        .ingest(&denied_id, "wrong-uid", wire::Format::Log, b"refused\n")
                        .await,
                    "wrong-uid",
                    class,
                );
                assert_eq!(
                    begin_import(client.capability(), &denied_id, "wrong-uid").await,
                    Err("PERMISSION_DENIED".into())
                );
                begin_import(client.capability(), &denied_id, "right-uid")
                    .await
                    .unwrap();
                client
                    .ingest(
                        &format!("right-{class}"),
                        "right-uid",
                        wire::Format::Log,
                        b"accepted\n",
                    )
                    .await
                    .unwrap();
            }
        })
        .await;
}

#[test]
fn source_bindings_reject_ambiguous_and_invalid_rules() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let config = write_server_config(dir.path(), &data, &data.join("rpc.sock"));
    let base = fs::read_to_string(&config).unwrap();
    for (rules, diagnostic) in [
        (
            r#"
[[source_bindings]]
source = "service."
prefix = true
allow = [{ class = "private-unix", uid = 1000 }]

[[source_bindings]]
source = "service.worker"
allow = [{ class = "chap" }]
"#,
            "overlap",
        ),
        (
            r#"
[[source_bindings]]
source = "remote"
allow = [{ class = "chap", uid = 1000 }]
"#,
            "unknown field",
        ),
    ] {
        fs::write(&config, format!("{base}\n{rules}")).unwrap();
        let Err(error) = sparrow_server::ServerConfig::read(&config) else {
            panic!("invalid source bindings were accepted: {rules}")
        };
        assert!(error.to_string().contains(diagnostic), "{error}");
    }
}
