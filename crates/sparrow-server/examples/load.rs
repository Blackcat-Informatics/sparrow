// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Concurrent typed ingestion and agent queries against a new private daemon.

use purrdf_lex::json::{Object, Value};
use sparrow_client::{Client, Endpoint, sparrow_capnp::Format};
use std::{cell::Cell, io::Read, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Traverse and release one frozen result using production renderers and cursors.
async fn capture(client: &Client, initial: &[&str]) -> Result<(usize, usize)> {
    let mut command = initial.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    command.extend(
        [
            "--source",
            "load-qualification",
            "--scan-budget",
            "100000",
            "--page-size",
            "1000",
        ]
        .map(str::to_owned),
    );
    let mut value = sparrow_server::run_cli(client, &command).await?;
    let snapshot = value["page"]["snapshot_id"]
        .as_str()
        .ok_or("missing capture identity")?
        .to_owned();
    let mut rows = 0;
    let mut pages = 0;
    loop {
        rows += value["rows"].as_array().ok_or("missing rows")?.len();
        pages += 1;
        if value["page"]["has_more"].as_bool() == Some(false) {
            break;
        }
        if pages >= 1024 {
            return Err("capture exceeded finite qualification page ceiling".into());
        }
        let cursor = value["page"]["next_cursor"]
            .as_str()
            .ok_or("missing continuation cursor")?
            .to_owned();
        value = sparrow_server::run_cli(
            client,
            &[
                "result-next".into(),
                client.discovery().store_id.clone(),
                cursor,
            ],
        )
        .await?;
    }
    client.release_result(&snapshot).await?;
    Ok((rows, pages))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let all = std::env::args().skip(1).collect::<Vec<_>>();
    let (endpoint, args) = Endpoint::from_args(&all)?;
    if args.len() != 4 {
        return Err("usage: load --secret-file PATH tcp://127.0.0.1:PORT NEW_INPUT_DIR BATCHES ROWS PADDING_BYTES (empty private daemon only)".into());
    }
    let Endpoint::AuthenticatedTcp(address, _) = &endpoint else {
        return Err("qualification requires an authenticated TCP endpoint".into());
    };
    if !(address.ip().is_loopback() && address.port() > 1024) {
        return Err(
            "qualification requires a private loopback TCP endpoint above port 1024".into(),
        );
    }
    let endpoint_label = format!("tcp://{address}");
    let batches: usize = args[1].parse()?;
    let rows: usize = args[2].parse()?;
    let padding: usize = args[3].parse()?;
    if !(1..=1000).contains(&batches)
        || !(1..=1000).contains(&rows)
        || padding > 65536
        || batches * (2 * rows + 2) > 50000
    {
        return Err(
            "require 1..1000 batches/rows, padding <=65536 and <=50000 total records".into(),
        );
    }
    tokio::task::LocalSet::new()
        .run_until(async {
            let producer = Client::connect_producer(&endpoint).await?;
            let reader = Client::connect_endpoint(&endpoint).await?;
            if producer.discovery().store_id != reader.discovery().store_id {
                return Err("connections reached different stores".into());
            }
            let before = sparrow_server::run_cli(&producer, &["status".into()]).await?;
            if before["live_records"].as_str() != Some("0")
                || before["active_pins"].as_str() != Some("0")
                || !before["consume_watches"]
                    .as_array()
                    .ok_or("missing watch state")?
                    .is_empty()
                || !before["syslog_listeners"]
                    .as_array()
                    .ok_or("missing listener state")?
                    .is_empty()
            {
                return Err("qualification refuses nonempty stores or configured producers".into());
            }
            let inputs = Path::new(&args[0]);
            std::fs::create_dir(inputs)?;
            let started = Instant::now();
            let done = Cell::new(false);
            let ingest = async {
                let mut measurements = Vec::new();
                let mut raw_bytes = 0_u64;
                let mut random = std::fs::File::open("/dev/urandom")?;
                for batch in 0..batches {
                    let mut jsonl = Vec::new();
                    for row in 0..rows {
                        jsonl.extend_from_slice(
                            purrdf_lex::json::write_compact(
                                &Object::new()
                                    .with("message", "loadprobe")
                                    .with("batch", batch)
                                    .with("row", row)
                                    .with("padding", "p".repeat(padding))
                                    .into(),
                            )
                            .as_bytes(),
                        );
                        jsonl.push(b'\n');
                    }
                    let log = format!("loadprobe batch={batch} {}\r\n", "p".repeat(padding))
                        .repeat(rows)
                        .into_bytes();
                    let markdown =
                        format!("# loadprobe\n\nBatch {batch}.\n{}\n", "p".repeat(padding))
                            .into_bytes();
                    let mut opaque = vec![0; padding.max(1024)];
                    random.read_exact(&mut opaque)?;
                    for (kind, bytes, expected) in [
                        (Format::Jsonl, jsonl, rows),
                        (Format::Log, log, rows),
                        (Format::Markdown, markdown, 1),
                        (Format::Opaque, opaque, 1),
                    ] {
                        let name = format!("load-{batch}-{}", sparrow_client::format_label(kind));
                        std::fs::write(inputs.join(&name), &bytes)?;
                        let began = Instant::now();
                        let receipt = producer
                            .ingest(&name, "load-qualification", kind, &bytes)
                            .await?;
                        let elapsed = began.elapsed().as_micros().to_string();
                        if receipt.record_ids.len() != expected
                            || producer
                                .bytes(&receipt.id, 0, bytes.len().try_into()?)
                                .await?
                                != bytes
                        {
                            return Err("accepted count or exact bytes differ from input".into());
                        }
                        raw_bytes += bytes.len() as u64;
                        measurements.push(Value::from(
                            Object::new()
                                .with("request", name)
                                .with("artifact", receipt.id)
                                .with("payload", receipt.payload_id)
                                .with("records", expected)
                                .with("bytes", bytes.len())
                                .with("ingest_us", elapsed)
                                .with("completed_us", started.elapsed().as_micros().to_string()),
                        ));
                    }
                }
                done.set(true);
                Ok::<_, Box<dyn std::error::Error>>((measurements, raw_bytes))
            };
            let query = async {
                let mut measurements = Vec::new();
                while !done.get() || measurements.is_empty() {
                    for initial in [
                        vec!["records"],
                        vec!["text", "loadprobe"],
                        vec!["keys"],
                        vec!["values", "--path", "/row", "--lexical", "0"],
                    ] {
                        let began = Instant::now();
                        let (count, pages) = capture(&reader, &initial).await?;
                        measurements.push(Value::from(
                            Object::new()
                                .with("operation", initial[0])
                                .with("rows", count)
                                .with("pages", pages)
                                .with("query_us", began.elapsed().as_micros().to_string())
                                .with("completed_us", started.elapsed().as_micros().to_string()),
                        ));
                    }
                }
                Ok::<_, Box<dyn std::error::Error>>(measurements)
            };
            let ((ingestion, raw_bytes), queries) = futures::try_join!(ingest, query)?;
            let mut final_queries = Vec::new();
            for (initial, expected) in [
                (vec!["records"], batches * (2 * rows + 2)),
                (vec!["text", "loadprobe"], batches * (2 * rows + 1)),
                (vec!["keys"], 5),
                (vec!["values", "--path", "/row", "--lexical", "0"], batches),
            ] {
                let began = Instant::now();
                let (count, pages) = capture(&reader, &initial).await?;
                if count != expected {
                    return Err(format!(
                        "final {} capture returned {count}, expected {expected}",
                        initial[0]
                    )
                    .into());
                }
                final_queries.push(Value::from(
                    Object::new()
                        .with("operation", initial[0])
                        .with("rows", count)
                        .with("pages", pages)
                        .with("query_us", began.elapsed().as_micros().to_string()),
                ));
            }
            let after = sparrow_server::run_cli(&producer, &["status".into()]).await?;
            if after["live_records"].as_str()
                != Some((batches * (2 * rows + 2)).to_string().as_str())
                || after["record_read_handles"].as_str() != Some("0")
                || after["record_read_bytes"].as_str() != Some("0")
            {
                return Err("final occurrence count or released capture accounting differs".into());
            }
            let report = Object::new()
                .with("complete", true)
                .with("endpoint", &endpoint_label)
                .with("batches", batches)
                .with("rows_per_batch", rows)
                .with("padding_bytes", padding)
                .with("accepted_raw_bytes", raw_bytes.to_string())
                .with("elapsed_us", started.elapsed().as_micros().to_string())
                .with("ingestion", Value::array(ingestion))
                .with("queries", Value::array(queries))
                .with("final_queries", Value::array(final_queries))
                .with("before", before)
                .with("after", after);
            println!("{}", purrdf_lex::json::write_pretty(&report.into()));
            Ok(())
        })
        .await
}
