// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Offline clock-injected retention qualification on a new owned store.

use purrdf_lex::json::{Object, Value};
use sparrow_core::EvictionLimits;
use sparrow_server::ServerConfig;
use std::{
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

struct Profile {
    json_rows: usize,
    log_rows: usize,
    opaque_bytes: usize,
}

impl Profile {
    fn records(&self, index: usize) -> usize {
        match index {
            0 => self.json_rows,
            1 => self.json_rows + 1,
            2 => self.log_rows,
            3 => 1,
            _ => unreachable!("four specified artifact kinds"),
        }
    }
}

struct Evidence {
    day: u64,
    index: usize,
    id: String,
    path: PathBuf,
    bytes: usize,
}

fn verify_bytes(
    store: &sparrow_core::Store,
    evidence: &Evidence,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(&evidence.path)?;
    if file.metadata()?.len() != evidence.bytes as u64 {
        return Err("saved input length changed".into());
    }
    let mut original = [0_u8; 64 * 1024];
    let mut offset = 0;
    while offset < evidence.bytes {
        let count = original.len().min(evidence.bytes - offset);
        file.read_exact(&mut original[..count])?;
        if store.bytes(&evidence.id, offset as u64, count, now)? != original[..count] {
            return Err("retained whole-artifact bytes differ".into());
        }
        offset += count;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if ![2, 5].contains(&args.len()) || !["none", "sparse", "dense"].contains(&args[1].as_str()) {
        return Err("usage: turnover CONFIG none|sparse|dense [JSON_ROWS LOG_ROWS OPAQUE_BYTES] (new offline store only)".into());
    }
    let config = ServerConfig::read(Path::new(&args[0]))?;
    let profile = if args.len() == 5 {
        Profile {
            json_rows: args[2].parse()?,
            log_rows: args[3].parse()?,
            opaque_bytes: args[4].parse()?,
        }
    } else {
        Profile {
            json_rows: 8,
            log_rows: 32,
            opaque_bytes: 64 * 1024,
        }
    };
    if profile.json_rows == 0
        || profile.json_rows >= config.max_records
        || profile.log_rows == 0
        || profile.log_rows > config.max_records
        || profile.opaque_bytes == 0
        || profile.opaque_bytes > config.max_artifact_bytes
    {
        return Err(
            "fixture sizes must be positive and fit configured artifact/record admission".into(),
        );
    }
    let daily_records = (0..4)
        .map(|i| profile.records(i))
        .try_fold(0_usize, |sum, count| sum.checked_add(count))
        .ok_or("fixture record count overflow")?;
    let max_passes = daily_records
        .checked_mul(120)
        .and_then(|n| n.checked_add(2048))
        .ok_or("fixture expiry ceiling overflow")?;
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
    let daily_path = input_dir.join("daily.jsonl");
    let rdf_path = input_dir.join("pinned-rdf.jsonl");
    let mut daily = BufWriter::new(std::fs::File::create_new(&daily_path)?);
    let mut rdf = BufWriter::new(std::fs::File::create_new(&rdf_path)?);
    let mut first_ids = Vec::new();
    let mut pin = None;
    let mut raw_bytes = 0_u64;
    let mut random = std::fs::File::open("/dev/urandom")?;
    for day in 0..60_u64 {
        let now = day * day_millis;
        let mut maintenance = Vec::new();
        for pass in 0..max_passes {
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
            if pass + 1 == max_passes {
                return Err("finite expiry traversal failed to complete".into());
            }
        }
        let repeated_row = b"{\"message\":\"repeated turnover content\",\"value\":1.00}\n";
        let repeated = repeated_row.repeat(profile.json_rows);
        let mut changing = Vec::new();
        for index in 0..profile.json_rows {
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
        let log = b"repeated turnover log record\r\n".repeat(profile.log_rows);
        let mut opaque = vec![0_u8; profile.opaque_bytes];
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
            let path = input_dir.join(format!("{day}-{index}"));
            std::fs::write(&path, &bytes)?;
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
            if receipt.record_ids.len() != profile.records(index) {
                return Err("accepted record count differs from fixture specification".into());
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
                    .with("records", profile.records(index)),
            ));
            if day == 0 {
                first_ids.extend(receipt.record_ids);
            }
            let evidence = Evidence {
                day,
                index,
                id: receipt.id,
                path,
                bytes: bytes.len(),
            };
            verify_bytes(&store, &evidence, now)?;
            inputs.push(evidence);
        }
        if args[1] == "dense" || (day == 0 && args[1] == "sparse") {
            let targets = if args[1] == "sparse" {
                vec![first_ids[0].clone()]
            } else {
                inputs[inputs.len() - 4..]
                    .iter()
                    .map(|evidence| evidence.id.clone())
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
            .map(|evidence| {
                if day < evidence.day + 30 || args[1] == "dense" {
                    profile.records(evidence.index)
                } else if evidence.day == 0 && evidence.index == 0 && args[1] == "sparse" {
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
        for evidence in &inputs {
            let whole = day < evidence.day + 30 || args[1] == "dense";
            if whole {
                verify_bytes(&store, evidence, now)?;
            } else if !matches!(store.bytes(&evidence.id, 0, 1, now), Err(ref e) if e.code == "EVIDENCE_UNAVAILABLE" || e.code == "NOT_FOUND")
            {
                return Err("expired parent remains downloadable".into());
            }
            if evidence.day == 0
                && evidence.index == 0
                && args[1] == "sparse"
                && store.bytes(&first_ids[0], 0, repeated_row.len(), now)? != repeated_row
            {
                return Err("sparse pinned record lost original bytes".into());
            }
        }
        if args[1] != "none" {
            for id in &first_ids {
                if args[1] == "dense" || id == &first_ids[0] {
                    writeln!(
                        rdf,
                        "{}",
                        purrdf_lex::json::write_compact(
                            &Object::new()
                                .with("day", day)
                                .with("record_id", id.as_str())
                                .with("inspection", store.rdf_inspect(id, now)?)
                                .into()
                        )
                    )?;
                }
            }
        }
        writeln!(
            daily,
            "{}",
            purrdf_lex::json::write_compact(&Value::from(
                Object::new()
                    .with("day", day)
                    .with("expected_records", expected)
                    .with("accepted", Value::array(accepted))
                    .with("maintenance", Value::array(maintenance))
                    .with(
                        "ingest_and_verify_seconds",
                        day_started.elapsed().as_secs_f64(),
                    )
                    .with("status", status),
            ))
        )?;
        daily.flush()?;
        rdf.flush()?;
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
    let expired = &first_ids[0];
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
        if final_passes >= max_passes {
            return Err("final finite expiry traversal failed to complete".into());
        }
    }
    store.flush()?;
    let final_status = store.status(90 * day_millis)?;
    for key in [
        "live_records",
        "active_pins",
        "unique_payload_chunks",
        "unique_json_projections",
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
        .with("complete", true).with("report_format", "turnover/2")
        .with("json_rows", profile.json_rows).with("log_rows", profile.log_rows).with("opaque_bytes", profile.opaque_bytes).with("records_per_day", daily_records).with("max_expiry_passes", max_passes)
        .with("method", "60 simulated daily cohorts; native retention/pins/bytes/RDF; no listeners; bounded expiry passes; reopen at days 29,30,59 and90; random opaque input; physical reclamation not certified")
        .with("pin_mode", args[1].as_str()).with("store_id", identity)
        .with("accepted_raw_bytes", raw_bytes.to_string()).with("elapsed_seconds", started.elapsed().as_secs_f64())
        .with("daily_path", daily_path.to_str().ok_or("non-UTF8 report path")?).with("pinned_rdf_path", rdf_path.to_str().ok_or("non-UTF8 report path")?).with("before_release", before_release)
        .with("final_expiry_passes", final_passes).with("final_status", final_status).into()));
    Ok(())
}
