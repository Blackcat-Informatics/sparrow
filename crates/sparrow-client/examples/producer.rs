// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Submit tagged events or drain an existing spool with COUNT=0.

use purrdf_lex::json::{self, Object, Value};
use sparrow_client::{
    Delivery, DeliveryConfig, DeliveryStats, DropReason, Endpoint, EventField, EventKind,
    EventLevel, FieldValue, FlushError, ProducerEvent, SendError, SpoolConfig,
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

fn lossy_progress(stats: &DeliveryStats, max_send_micros: u64, flush: Option<&str>) -> String {
    let mut dropped = Object::new();
    for (reason, key) in [
        (DropReason::Full, "full"),
        (DropReason::Contended, "contended"),
        (DropReason::Invalid, "invalid"),
        (DropReason::Closed, "closed"),
        (DropReason::Refused, "refused"),
        (DropReason::Late, "late"),
        (DropReason::CounterExhausted, "counter_exhausted"),
    ] {
        dropped = dropped.with(key, stats.dropped_by_reason[reason as usize]);
    }
    let mut object = Object::new()
        .with("instance", purrdf_hash::hex::encode(&stats.client_instance))
        .with("offered", stats.offered_events)
        .with("queued", stats.admitted_events)
        .with("dropped", dropped)
        .with(
            "dropped_total",
            stats
                .dropped_by_reason
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add),
        )
        .with("max_send_micros", max_send_micros);
    if let Some(outcome) = flush {
        object = object
            .with("flush", outcome)
            .with("durable_sequence", stats.last_durable_sequence)
            .with("durable_events", stats.durable_events);
    }
    json::write_compact(&object.into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    const USAGE: &str = "usage: producer ENDPOINT SOURCE lossy COUNT RATE TAG | producer ENDPOINT SOURCE lossless SPOOL_DIR COUNT RATE TAG [--spool-max-bytes N]";
    let [endpoint, source, mode, rest @ ..] = args.as_slice() else {
        return Err(USAGE.into());
    };
    let endpoint = Endpoint::parse(endpoint)?;
    let lossy = mode == "lossy";
    let (config, inputs) = match mode.as_str() {
        "lossy" if rest.len() == 3 => (DeliveryConfig::lossy(endpoint, source), rest),
        "lossless" if rest.len() == 4 || rest.len() == 6 && rest[4] == "--spool-max-bytes" => {
            let mut spool = SpoolConfig::new(std::path::absolute(&rest[0])?);
            if let Some(raw) = rest.get(5) {
                spool.max_bytes = raw.parse()?;
            }
            (
                DeliveryConfig::lossless(endpoint, source, spool),
                &rest[1..4],
            )
        }
        _ => return Err(USAGE.into()),
    };
    let count: u64 = inputs[0].parse()?;
    let rate: u64 = inputs[1].parse()?;
    let tag = &inputs[2];
    if rate == 0 {
        return Err("RATE must be positive".into());
    }
    let delivery = Delivery::start_async(config).await?;
    let mut max_send_micros = 0;
    let start = tokio::time::Instant::now();
    let mut report = start + Duration::from_secs(1);
    for index in 0..count {
        let event = ProducerEvent {
            sequence: 0,
            event_time: sparrow_client::unix_millis_now()?,
            kind: EventKind::Log,
            level: EventLevel::Info,
            name: tag.clone(),
            target: "sparrow.example".to_owned(),
            fields: vec![EventField {
                name: "message".into(),
                value: FieldValue::Text(format!("{tag} {}", index + 1)),
            }],
            context: vec![],
            span: None,
            follows_from: None,
        };
        if lossy {
            let before = std::time::Instant::now();
            delivery.try_send(event);
            max_send_micros = max_send_micros
                .max(u64::try_from(before.elapsed().as_micros()).unwrap_or(u64::MAX));
        } else if let Err(SendError::Uncertain { sequence }) = delivery.send(event).await {
            return Err(format!("recover local spool sequence {sequence}").into());
        }
        if tokio::time::Instant::now() >= report {
            let stats = delivery.stats()?;
            eprintln!(
                "{}",
                if lossy {
                    lossy_progress(&stats, max_send_micros, None)
                } else {
                    progress(&stats, "pending")
                }
            );
            report = tokio::time::Instant::now() + Duration::from_secs(1);
        }
        let elapsed = Duration::try_from_secs_f64((index + 1) as f64 / rate as f64)?;
        let due = start
            .checked_add(elapsed)
            .ok_or("rate schedule exceeds the supported clock range")?;
        if due > tokio::time::Instant::now() {
            tokio::time::sleep_until(due).await;
        }
    }
    if lossy {
        let outcome = match delivery.shutdown_async(Duration::from_secs(60)).await {
            Ok(_) => "ok",
            Err(FlushError::Dropped(_)) => "dropped",
            Err(FlushError::TimedOut(_)) => "timed_out",
            Err(error) => return Err(error.into()),
        };
        println!(
            "{}",
            lossy_progress(&delivery.stats()?, max_send_micros, Some(outcome))
        );
        return Ok(());
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
