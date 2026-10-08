// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One typed receiver-window lookup and renderer shared by CLI and MCP.

use super::{Result, failure, instant, invalid, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub id: String,
}
impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        if !self.id.starts_with("syslog-receiver:") || self.id.len() > 128 {
            return Err(invalid(
                "receiver window requires an opaque syslog-receiver ID",
            ));
        }
        sparrow_client::Discovery::fetch(client).await?;
        let mut request = client.receiver_window_get_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_id(&self.id);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        let response = request.send().promise.await?;
        let window = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let gaps = window
            .get_gaps()?
            .iter()
            .map(|gap| {
                Value::from(
                    Object::new()
                        .with("first", gap.get_first().to_string())
                        .with("last", gap.get_last().to_string()),
                )
            })
            .collect::<Vec<_>>();
        Ok(Object::new()
            .with("store_id", window.get_store_id()?.to_str()?)
            .with("window_id", window.get_id()?.to_str()?)
            .with("source", window.get_source()?.to_str()?)
            .with("listener", window.get_listener()?.to_str()?)
            .with("instance", window.get_instance()?.to_str()?)
            .with(
                "transport",
                match window.get_transport()? {
                    wire::ReceiverTransport::Udp => "udp",
                    wire::ReceiverTransport::TcpOctetCounting => "tcp_octet_counting",
                    wire::ReceiverTransport::TcpLf => "tcp_lf",
                    wire::ReceiverTransport::TcpCrLf => "tcp_cr_lf",
                },
            )
            .with("observations", window.get_observations().to_string())
            .with("accepted", window.get_accepted().to_string())
            .with("discarded", window.get_discarded().to_string())
            .with("refused", window.get_refused().to_string())
            .with("truncated", window.get_truncated().to_string())
            .with(
                "framing_failures",
                window.get_framing_failures().to_string(),
            )
            .with("timed_out", window.get_timed_out().to_string())
            .with("gaps", gaps)
            .with("gaps_complete", window.get_gaps_complete())
            .with("continuity_unknown", window.get_continuity_unknown())
            .with(
                "pre_commit_loss_unknown",
                window.get_pre_commit_loss_unknown(),
            )
            .with(
                "first_received_at",
                instant(window.get_first_received_at()?)?,
            )
            .with("last_received_at", instant(window.get_last_received_at()?)?)
            .with("expires_at", instant(window.get_expires_at()?)?)
            .into())
    }
}
