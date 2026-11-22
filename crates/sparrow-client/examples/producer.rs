// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Submit tagged events or drain an existing spool with COUNT=0.

use purrdf_lex::json::{self, Object, Value};
use sparrow_client::{
    Delivery, DeliveryConfig, DeliveryStats, Endpoint, EventKind, EventLevel, ProducerEvent,
    SendError, SpoolConfig,
};
use std::time::Duration;

fn progress(stats: &DeliveryStats, flush: &str) -> String {
    let mut refused = Object::new();
    for (reason, count) in ["spool_full", "busy", "invalid", "closed", "spool_failed"]
        .into_iter()
        .zip(stats.refused_by_reason)
    {
        refused = refused.with(reason, count);
    }
    let mut object = Object::new()
        .with("offered", stats.offered_events)
        .with("durable_sequence", stats.last_durable_sequence)
        .with("refused", refused)
        .with("flush", flush);
    if let Some(spool) = &stats.spool {
        object = object
            .with("spooled_sequence", spool.spooled_sequence)
            .with("spool_bytes", spool.bytes);
    }
    let held = stats.held.as_ref().map_or(Value::Null, |error| {
        let code = match error.as_ref() {
            sparrow_client::Error::Domain { code, .. } => code.as_str(),
            sparrow_client::Error::Unsupported(_) => "UNSUPPORTED",
            _ => "DELIVERY_HELD",
        };
        Object::new()
            .with("code", code)
            .with("message", error.to_string())
            .with(
                "batch",
                stats
                    .spool
                    .as_ref()
                    .map(|s| s.next_batch)
                    .map_or(Value::Null, Value::from),
            )
            .into()
    });
    json::write_compact(&object.with("held", held).into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(args.len() == 7 || args.len() == 9 && args[7] == "--spool-max-bytes")
        || args[2] != "lossless"
    {
        return Err("usage: producer ENDPOINT SOURCE lossless SPOOL_DIR COUNT RATE TAG [--spool-max-bytes N]".into());
    }
    let mut spool = SpoolConfig::new(std::path::absolute(&args[3])?);
    if let Some(raw) = args.get(8) {
        spool.max_bytes = raw.parse()?;
    }
    let count: u64 = args[4].parse()?;
    let rate: u64 = args[5].parse()?;
    if rate == 0 {
        return Err("RATE must be positive".into());
    }
    let delivery = Delivery::start_async(DeliveryConfig::lossless(
        Endpoint::parse(&args[0])?,
        &args[1],
        spool,
    ))
    .await?;
    let start = tokio::time::Instant::now();
    let mut report = start + Duration::from_secs(1);
    for index in 0..count {
        let event = ProducerEvent {
            sequence: 0,
            event_time: sparrow_client::unix_millis_now()?,
            kind: EventKind::Log,
            level: EventLevel::Info,
            name: args[6].clone(),
            target: "sparrow.example".to_owned(),
            fields: vec![],
            context: vec![],
            span: None,
            follows_from: None,
        };
        if let Err(SendError::Uncertain { sequence }) = delivery.send(event).await {
            return Err(format!("recover local spool sequence {sequence}").into());
        }
        if tokio::time::Instant::now() >= report {
            eprintln!("{}", progress(&delivery.stats()?, "pending"));
            report = tokio::time::Instant::now() + Duration::from_secs(1);
        }
        let due = start + Duration::from_secs_f64((index + 1) as f64 / rate as f64);
        if due > tokio::time::Instant::now() {
            tokio::time::sleep_until(due).await;
        }
    }
    delivery.flush_local(Duration::from_secs(10)).await?;
    let flush = match delivery.shutdown_async(Duration::from_secs(10)).await {
        Ok(_) => "ok",
        Err(sparrow_client::FlushError::TimedOut(stats)) => {
            if stats.held.is_some() {
                "held"
            } else {
                "timed_out"
            }
        }
        Err(error) => return Err(error.into()),
    };
    let stats = delivery.stats()?;
    println!("{}", progress(&stats, flush));
    if flush != "ok" || stats.refused_by_reason.iter().any(|&count| count > 0) {
        return Err("producer run ended with refused input or an incomplete remote drain".into());
    }
    Ok(())
}
