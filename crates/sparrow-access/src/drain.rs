// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use purrdf_lex::json::{Object, Value};
use sparrow_client::{Delivery, Endpoint, FlushError};
use std::{num::NonZeroU64, time::Duration};

pub(super) async fn run(
    endpoint: Endpoint,
    args: &[String],
) -> Result<Value, Box<dyn std::error::Error>> {
    let usage = "usage: sparrow ENDPOINT spool-drain ABSOLUTE_DIRECTORY [--timeout-secs SECONDS]";
    let (directory, seconds) = match args {
        [directory] => (directory, 30),
        [directory, flag, value] if flag == "--timeout-secs" => (
            directory,
            value.parse::<NonZeroU64>().map_err(|_| usage)?.get(),
        ),
        _ => return Err(usage.into()),
    };
    let proof = Delivery::drain_spool(endpoint, directory, Duration::from_secs(seconds))
        .await
        .map_err(|error| {
            let mut message = format!("cannot drain spool {directory}: {error}");
            if let FlushError::TimedOut(stats) = &error
                && let Some(reason) = stats
                    .held
                    .as_ref()
                    .or(stats.last_transient_failure.as_ref())
            {
                message.push_str(&format!(" ({reason})"));
            }
            message
        })?;
    Ok(Object::new()
        .with(
            "client_instance",
            purrdf_hash::hex::encode(&proof.client_instance),
        )
        .with("required_sequence", proof.required_sequence.to_string())
        .with("durable_sequence", proof.durable_sequence.to_string())
        .with("durable_events", proof.durable_events.to_string())
        .into())
}
