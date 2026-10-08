// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Send one declared typed event over Unix or TCP; exact retries reuse its receipt.

use sparrow_client::{
    Client, Durability, EventBatch, EventField, EventKind, EventLevel, FieldValue, ProducerEvent,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !(6..=8).contains(&args.len()) || args[3].len() != 32 {
        return Err(std::io::Error::other("usage: events SOCKET SOURCE REQUEST_ID INSTANCE_HEX UNIX_MILLIS MESSAGE [buffered|durable [SEQUENCE]]; sequence defaults to 1; instance is 16 bytes in canonical hex; retries keep event arguments identical").into());
    }
    let durability = match args.get(6).map(String::as_str) {
        None | Some("durable") => Durability::Durable,
        Some("buffered") => Durability::Buffered,
        Some(_) => {
            return Err(std::io::Error::other("durability requires buffered or durable").into());
        }
    };
    let instance = purrdf_hash::hex::decode_canonical(&args[3])?;
    let batch = EventBatch {
        request_id: args[2].clone(),
        source: args[1].clone(),
        client_instance: instance
            .try_into()
            .map_err(|_| std::io::Error::other("instance must contain 16 bytes"))?,
        events: vec![ProducerEvent {
            sequence: args
                .get(7)
                .map(|sequence| sequence.parse())
                .transpose()?
                .unwrap_or(1),
            event_time: args[4].parse()?,
            kind: EventKind::Log,
            level: EventLevel::Info,
            name: "typed_example".into(),
            target: "sparrow.example".into(),
            fields: vec![
                EventField {
                    name: "message".into(),
                    value: FieldValue::Text(args[5].clone()),
                },
                EventField {
                    name: "typed".into(),
                    value: FieldValue::Boolean(true),
                },
            ],
            context: vec![],
            span: None,
            follows_from: None,
        }],
    };
    tokio::task::LocalSet::new()
        .run_until(async {
            let client =
                Client::connect_endpoint(&sparrow_client::Endpoint::parse(&args[0])?).await?;
            let receipt = client.events_with_durability(&batch, durability).await?;
            println!(
                "{} {:?} records={} bytes={}",
                receipt.id,
                receipt.durability,
                receipt.record_ids.len(),
                receipt.byte_length
            );
            if let Some(token) = receipt.buffered_through {
                println!("buffered_through={token:?}");
            }
            Ok::<_, sparrow_client::Error>(())
        })
        .await?;
    Ok(())
}
