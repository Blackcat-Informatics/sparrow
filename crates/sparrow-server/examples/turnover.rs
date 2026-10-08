// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Offline clock-injected retention qualification on a new owned store.

use purrdf_lex::json::{Object, Value};
use sparrow_core::EvictionLimits;
use sparrow_server::ServerConfig;
use std::{io::Read, path::Path, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 || !["none", "sparse", "dense"].contains(&args[1].as_str()) {
        return Err("usage: turnover CONFIG none|sparse|dense (new offline store only)".into());
    }
    let config = ServerConfig::read(Path::new(&args[0]))?;
    if config.data_dir.exists()
        || !config.consume.is_empty()
        || !config.syslog.is_empty()
        || config.rpc_tcp_address.is_some()
    {
        return Err(
            "turnover requires a nonexistent store and no configured intake or TCP binding".into(),
        );
    }
    let day_millis = sparrow_core::parse_instant("1970-01-02T00:00:00Z")?;
    let input_dir = config.data_dir.with_extension("inputs");
    std::fs::create_dir(&input_dir)?;
    let started = Instant::now();
    let mut store = config.open_store()?;
    let identity = store.store_id()?;
    let mut inputs = Vec::new();
    let mut daily = Vec::new();
    let mut pin = None;
    let mut raw_bytes = 0_u64;
    let mut random = std::fs::File::open("/dev/urandom")?;
    for day in 0..60_u64 {
        let now = day * day_millis;
        let mut maintenance = Vec::new();
        for pass in 0..1024 {
            let report = store.expire_bounded(now, EvictionLimits::default())?;
            maintenance.push(Value::from(
                Object::new()
                    .with("visited", report.visited)
                    .with("retired_records", report.records.to_string())
                    .with("retired_artifacts", report.artifacts.to_string())
                    .with("continuation", report.continuation),
            ));
            if !report.continuation {
                break;
            }
            if pass == 1023 {
                return Err("finite expiry traversal failed to complete".into());
            }
        }
        let repeated = b"{\"message\":\"repeated turnover content\",\"value\":1.00}\n".repeat(8);
        let mut changing = Vec::new();
        for index in 0..8_u32 {
            changing.extend_from_slice(
                purrdf_lex::json::write_compact(
                    &Object::new()
                        .with("day", day.to_string())
                        .with("index", index)
                        .with("message", "changing turnover content")
                        .into(),
                )
                .as_bytes(),
            );
            changing.push(b'\n');
        }
        changing.extend_from_slice(b"{malformed turnover record\n");
        let log = b"repeated turnover log record\r\n".repeat(32);
        let mut opaque = vec![0_u8; 64 * 1024];
        random.read_exact(&mut opaque)?;
        let day_started = Instant::now();
        let mut accepted = Vec::new();
        for (index, (format, bytes)) in [
            ("jsonl", repeated),
            ("jsonl", changing),
            ("log", log),
            ("opaque", opaque),
        ]
        .into_iter()
        .enumerate()
        {
            std::fs::write(input_dir.join(format!("{day}-{index}")), &bytes)?;
            let receipt = store.ingest(
                &format!("turnover-{day}-{index}"),
                "turnover",
                format,
                &bytes,
                now,
            )?;
            raw_bytes += bytes.len() as u64;
            if receipt.expires_at != now + 30 * day_millis {
                return Err("ordinary retention deadline differs from 30 days".into());
            }
            if store.bytes(&receipt.id, 0, bytes.len(), now)? != bytes {
                return Err("accepted artifact bytes differ".into());
            }
            let digest = format!(
                "blake3:{}",
                purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&bytes).as_bytes())
            );
            if receipt.payload_id != digest {
                return Err("accepted artifact content identity differs".into());
            }
            accepted.push(Value::from(
                Object::new()
                    .with("artifact_id", receipt.id.as_str())
                    .with("payload_id", digest)
                    .with("format", format)
                    .with("bytes", bytes.len().to_string())
                    .with("records", receipt.record_ids.len()),
            ));
            inputs.push((day, index, receipt, bytes));
        }
        if args[1] == "dense" || (day == 0 && args[1] == "sparse") {
            let targets = if args[1] == "sparse" {
                vec![inputs[0].2.record_ids[0].clone()]
            } else {
                inputs[inputs.len() - 4..]
                    .iter()
                    .map(|(_, _, receipt, _)| receipt.id.clone())
                    .collect()
            };
            pin = Some(store.pin(
                &format!("turnover-pin-{day}"),
                "turnover",
                &targets,
                90,
                now,
            )?);
        }
        let expected: usize = inputs
            .iter()
            .map(|(accepted_day, index, receipt, _)| {
                if day < accepted_day + 30 || args[1] == "dense" {
                    receipt.record_ids.len()
                } else if *accepted_day == 0 && *index == 0 && args[1] == "sparse" {
                    1
                } else {
                    0
                }
            })
            .sum();
        let status = store.status(now)?;
        if status["live_records"].as_str() != Some(expected.to_string().as_str()) {
            return Err(format!(
                "day {day}: live count differs from independent deadline/pin oracle"
            )
            .into());
        }
        for (accepted_day, index, receipt, bytes) in &inputs {
            let whole = day < accepted_day + 30 || args[1] == "dense";
            let result = store.bytes(&receipt.id, 0, bytes.len(), now);
            if whole {
                if result? != *bytes {
                    return Err("retained whole-artifact bytes differ".into());
                }
            } else if !matches!(result, Err(ref e) if e.code == "EVIDENCE_UNAVAILABLE" || e.code == "NOT_FOUND")
            {
                return Err("expired parent remains downloadable".into());
            }
            if *accepted_day == 0 && *index == 0 && args[1] == "sparse" {
                let record = store.record(&receipt.record_ids[0])?;
                if store.bytes(
                    &record.id,
                    0,
                    (record.byte_end - record.byte_start) as usize,
                    now,
                )? != bytes[record.byte_start as usize..record.byte_end as usize]
                {
                    return Err("sparse pinned record lost original bytes".into());
                }
                store.rdf_inspect(&record.id, now)?;
            }
        }
        let mut pinned_rdf = Vec::new();
        if args[1] != "none" {
            for (_, _, receipt, _) in &inputs[..4] {
                for id in &receipt.record_ids {
                    if args[1] == "dense" || id == &inputs[0].2.record_ids[0] {
                        pinned_rdf.push(store.rdf_inspect(id, now)?);
                    }
                }
            }
        }
        daily.push(Value::from(
            Object::new()
                .with("day", day)
                .with("expected_records", expected)
                .with("accepted", Value::array(accepted))
                .with("maintenance", Value::array(maintenance))
                .with("pinned_rdf", Value::array(pinned_rdf))
                .with(
                    "ingest_and_verify_seconds",
                    day_started.elapsed().as_secs_f64(),
                )
                .with("status", status),
        ));
        if day == 29 || day == 30 || day == 59 {
            store.flush()?;
            drop(store);
            store = config.open_store()?;
            if store.store_id()? != identity
                || store.status(now)?["live_records"].as_str()
                    != Some(expected.to_string().as_str())
            {
                return Err("reopen changed retained identity or membership".into());
            }
        }
    }
    let before_release = store.status(60 * day_millis)?;
    if let Some(pin) = pin {
        store.release(&pin.id)?;
    }
    let expired = &inputs[0].2.record_ids[0];
    if !matches!(store.bytes(expired, 0, 1, 60 * day_millis), Err(ref e) if e.code == "EVIDENCE_UNAVAILABLE" || e.code == "NOT_FOUND")
    {
        return Err("released expired evidence remains eligible".into());
    }
    if !matches!(store.pin("turnover-resurrect", "resurrect", std::slice::from_ref(expired), 1, 60 * day_millis), Err(ref e) if e.code == "EVIDENCE_UNAVAILABLE" || e.code == "NOT_FOUND")
    {
        return Err("release allowed expired evidence resurrection".into());
    }
    let mut final_passes = 0;
    loop {
        final_passes += 1;
        let report = store.expire_bounded(90 * day_millis, EvictionLimits::default())?;
        if !report.continuation {
            break;
        }
        if final_passes >= 1024 {
            return Err("final finite expiry traversal failed to complete".into());
        }
    }
    store.flush()?;
    let final_status = store.status(90 * day_millis)?;
    for key in [
        "live_records",
        "active_pins",
        "unique_payload_chunks",
        "record_read_handles",
        "record_read_bytes",
    ] {
        if final_status[key].as_str() != Some("0") {
            return Err(format!("final {key} is not zero").into());
        }
    }
    drop(store);
    let reopened = config.open_store()?;
    if reopened.store_id()? != identity
        || reopened.status(90 * day_millis)?["live_records"].as_str() != Some("0")
    {
        return Err("final reopen resurrected retired evidence".into());
    }
    println!("{}", purrdf_lex::json::write_compact(&Object::new()
        .with("method", "60 simulated daily cohorts; native retention/pins/bytes/RDF; no listeners; bounded expiry passes; reopen at days 29,30,59 and90; random opaque input; physical reclamation not certified")
        .with("pin_mode", args[1].as_str()).with("store_id", identity)
        .with("accepted_raw_bytes", raw_bytes.to_string()).with("elapsed_seconds", started.elapsed().as_secs_f64())
        .with("daily", Value::array(daily)).with("before_release", before_release)
        .with("final_expiry_passes", final_passes).with("final_status", final_status).into()));
    Ok(())
}
