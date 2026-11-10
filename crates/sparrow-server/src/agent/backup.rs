// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! `sparrow SOCKET backup` and the backup state `status` reports (#156).

use super::{Result, failure, wire};
use purrdf_lex::json::{Object, Value};

/// Start an online backup into the daemon's configured backup root; it returns at once with
/// the running state, and `status` reports the outcome.
pub(super) async fn execute(client: &wire::sparrow::Client) -> Result<Value> {
    let discovery = sparrow_client::Discovery::fetch(client).await?;
    let mut request = client.backup_request();
    request.get().set_store_id(&discovery.store_id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => render(value?),
        wire::result::Error(error) => Err(failure(error?)),
    }
}

fn instant(millis: u64) -> Result<Value> {
    if millis == 0 {
        return Ok(Value::Null);
    }
    Ok(sparrow_core::format_instant(millis)
        .map_err(|error| super::invalid(error.message))?
        .into())
}

/// The backup state as CLI and MCP render it.
pub(super) fn render(value: wire::backup_status::Reader<'_>) -> Result<Value> {
    Ok(Object::new()
        .with(
            "phase",
            match value.get_phase()? {
                wire::BackupPhase::Unconfigured => "unconfigured",
                wire::BackupPhase::Idle => "idle",
                wire::BackupPhase::Running => "running",
                wire::BackupPhase::Complete => "complete",
                wire::BackupPhase::Failed => "failed",
            },
        )
        .with("root", value.get_root()?.to_str()?)
        .with("keep", u64::from(value.get_keep()))
        .with("started_at", instant(value.get_started_at())?)
        .with("finished_at", instant(value.get_finished_at())?)
        .with("backup_id", u64::from(value.get_backup_id()))
        .with("files", u64::from(value.get_files()))
        .with("bytes", value.get_bytes().to_string())
        .with(
            "failure",
            match value.get_failure()?.which()? {
                wire::optional_text::Absent(()) => Value::Null,
                wire::optional_text::Value(text) => Value::from(text?.to_str()?),
            },
        )
        .into())
}
