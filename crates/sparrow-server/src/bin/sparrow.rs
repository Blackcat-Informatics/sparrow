// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Agent-facing CLI over the daemon's generated typed client.

mod query;

use purrdf_lex::json::{self, Object};
use sparrow_server::{connect, sparrow_capnp::result};
use std::path::Path;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        return Err(query::USAGE.into());
    }
    let command = query::Command::parse(&args[1..])?;
    tokio::task::LocalSet::new()
        .run_until(async {
            let connection = connect(Path::new(&args[0])).await?;
            if let Some(output) = command.execute(&connection.client).await? {
                println!("{}", json::write_compact(&output));
                return Ok(());
            }
            let response = connection.client.status_request().send().promise.await?;
            let value = match response.get()?.get_result()?.which()? {
                result::Ok(value) => value?,
                result::Error(error) => {
                    let error = error?;
                    return Err(capnp::Error::failed(format!(
                        "{}: {}",
                        error.get_code()?.to_str()?,
                        error.get_message()?.to_str()?
                    )));
                }
            };
            let output = Object::new()
                .with("store_id", value.get_store_id()?.to_str()?)
                .with("store_format", value.get_store_format()?.to_str()?)
                .with("schema_version", u64::from(value.get_schema_version()))
                .with("physical_bytes", value.get_physical_bytes().to_string())
                .with("disk_budget", value.get_disk_budget().to_string())
                .with(
                    "maintenance_reserve",
                    value.get_maintenance_reserve().to_string(),
                )
                .with("live_records", value.get_live_records().to_string())
                .with(
                    "unique_payload_chunks",
                    value.get_unique_payload_chunks().to_string(),
                )
                .with(
                    "unique_json_projections",
                    value.get_unique_json_projections().to_string(),
                )
                .with("active_pins", value.get_active_pins().to_string())
                .with(
                    "retry_window_seconds",
                    value.get_retry_window_seconds().to_string(),
                )
                .with(
                    "max_artifact_bytes",
                    value.get_max_artifact_bytes().to_string(),
                )
                .with(
                    "max_records_per_artifact",
                    value.get_max_records_per_artifact().to_string(),
                )
                .with("max_pin_members", value.get_max_pin_members().to_string());
            println!("{}", json::write_compact(&output.into()));
            Ok::<(), capnp::Error>(())
        })
        .await?;
    Ok(())
}
