// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One application event through the production bounded delivery worker.

use sparrow_client::{
    Admission, Delivery, DeliveryConfig, EventField, EventKind, EventLevel, FieldValue,
    ProducerEvent,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            std::io::Error::other("usage: delivery SOCKET SOURCE UNIX_MILLIS MESSAGE").into(),
        );
    }
    let delivery = Delivery::start(DeliveryConfig::new(&args[0], &args[1]))?;
    let admission = delivery.try_send(ProducerEvent {
        sequence: 0,
        event_time: args[2].parse()?,
        kind: EventKind::Log,
        level: EventLevel::Info,
        name: "delivery_example".into(),
        target: "sparrow.example".into(),
        fields: vec![EventField {
            name: "message".into(),
            value: FieldValue::Text(args[3].clone()),
        }],
        context: vec![],
        span: None,
        follows_from: None,
    });
    if !matches!(admission, Admission::Queued { .. }) {
        return Err(
            std::io::Error::other(format!("delivery admission refused: {admission:?}")).into(),
        );
    }
    let watermark = delivery.shutdown(std::time::Duration::from_secs(5))?;
    let receipt = watermark
        .last_receipt
        .ok_or_else(|| std::io::Error::other("no durable receipt for admitted event"))?;
    println!(
        "{} Durable sequence={} events={} instance={}",
        receipt.id,
        watermark.durable_sequence,
        watermark.durable_events,
        purrdf_hash::hex::encode(&watermark.client_instance)
    );
    Ok(())
}
