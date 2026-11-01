// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Verify explicit evidence in a restored, offline store without activating intake.

use purrdf_lex::json::{Object, Value};
use sparrow_server::ServerConfig;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !(4..=1027).contains(&args.len()) {
        return Err("usage: recovery CONFIG EXPECTED_STORE RFC3339 EVIDENCE_ID... (1..1024 targets; use a restored copy)".into());
    }
    let now = sparrow_core::parse_instant(&args[2])?;
    let config = ServerConfig::read(Path::new(&args[0]))?;
    // Opening may perform required format upgrades. Never run on the backup
    // itself. No ConsumeWorker, listener, maintenance or source path is opened.
    let store = config.open_store()?;
    if store.store_id()? != args[1] {
        return Err("restored store identity differs from expected store".into());
    }
    let mut targets = Vec::new();
    let mut total = 0_u64;
    let mut seen = std::collections::BTreeSet::new();
    for id in &args[3..] {
        if !seen.insert(id) {
            return Err("duplicate recovery target".into());
        }
        let (length, expected) = if id.starts_with("artifact:") {
            let artifact = store.artifact(id)?;
            (artifact.byte_length, Some(artifact.payload_id))
        } else if id.starts_with("record:") {
            let record = store.record(id)?;
            (
                record
                    .byte_end
                    .checked_sub(record.byte_start)
                    .ok_or("invalid record range")?,
                None,
            )
        } else if id.starts_with("file-generation:") {
            let generation = store.tail_generation(id)?;
            let finish = generation
                .finish
                .ok_or("generation has no producer finish")?;
            if finish.finished_at.is_none() {
                return Err("generation is not terminal".into());
            }
            (finish.intent.observed.byte_length, None)
        } else {
            return Err(
                "recovery targets must be artifacts, records or finished file generations".into(),
            );
        };
        total = total
            .checked_add(length)
            .ok_or("recovery byte count overflow")?;
        if total > 1024 * 1024 * 1024 {
            return Err("recovery targets exceed the 1 GiB read budget".into());
        }
        targets.push((id, length, expected));
    }
    let mut rows = Vec::new();
    for (id, length, expected) in targets {
        let mut hash = purrdf_hash::blake3::Hasher::new();
        let mut offset = 0;
        // A zero-byte read still checks retention and complete-file eligibility.
        store.bytes(id, 0, 0, now)?;
        while offset < length {
            let size = usize::try_from((length - offset).min(1024 * 1024))?;
            let bytes = store.bytes(id, offset, size, now)?;
            if bytes.len() != size {
                return Err("restored byte read returned an incomplete range".into());
            }
            hash.update(&bytes);
            offset += u64::try_from(size)?;
        }
        let payload = format!(
            "blake3:{}",
            purrdf_hash::hex::encode(hash.finalize().as_bytes())
        );
        if expected
            .as_ref()
            .is_some_and(|expected| expected != &payload)
        {
            return Err(format!("restored artifact payload mismatch: {id}").into());
        }
        let mut row = Object::new()
            .with("id", id.as_str())
            .with("bytes", length.to_string())
            .with("payload_id", payload);
        if id.starts_with("record:") {
            row = row.with("rdf", store.rdf_inspect(id, now)?);
        }
        rows.push(Value::from(row));
    }
    let report = Object::new()
        .with("store_id", store.store_id()?)
        .with("verified_bytes", total.to_string())
        .with("targets", rows)
        .with("status", store.status(now)?);
    println!("{}", purrdf_lex::json::write_compact(&report.into()));
    Ok(())
}
