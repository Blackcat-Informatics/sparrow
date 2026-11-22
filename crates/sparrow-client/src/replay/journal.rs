// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Crash checkpoints for an operator replay. The directory lock survives file replacement.

use super::{Parsed, corrupt, parse, read_file, replay_parts};
use crate::{
    ArtifactReceipt, Client, Durability, Error, EventBatch, Format,
    receipt::{Contract, ExpectedArtifact},
    sparrow_capnp::artifact,
};
use purrdf_lex::json::{self, Object, Value};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

pub(super) struct Journal {
    path: PathBuf,
    directory: crate::ExclusiveFileLock,
}

impl Journal {
    pub(super) fn open(path: PathBuf) -> Result<(Arc<Self>, Parsed), Error> {
        let parent = path.parent().ok_or_else(|| corrupt("file has no parent"))?;
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"))
        {
            return Err(corrupt(
                "select the published quarantine file, not its scratch name",
            ));
        }
        let directory = crate::ExclusiveFileLock::acquire(File::open(parent)?)?;
        let (bytes, metadata) = read_file(&path)?;
        let parsed = parse(&bytes)?;
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() == 2 {
            crate::quarantine::recover_scratch(
                &path,
                &parsed.batch,
                &metadata,
                directory.as_ref(),
            )?;
        }
        if parsed.header.get("profile").and_then(Value::as_str)
            == Some("sparrow.client.quarantine/2")
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path)?;
            if parsed.valid_bytes < bytes.len() {
                file.set_len(parsed.valid_bytes as u64)?;
            }
            // An earlier failed sync can leave a complete attempt visible only in
            // the page cache. Establish both its bytes and its name before retrying.
            file.sync_all()?;
            directory.as_ref().sync_all()?;
        }
        Ok((Arc::new(Self { path, directory }), parsed))
    }

    pub(super) async fn remove(self: &Arc<Self>) -> Result<(), Error> {
        let journal = self.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::remove_file(&journal.path)?;
            journal.directory.as_ref().sync_all()
        })
        .await
        .map_err(std::io::Error::other)??;
        Ok(())
    }

    pub(super) async fn save(self: &Arc<Self>, parsed: &Parsed) -> Result<(), Error> {
        let journal = self.clone();
        let mut header = parsed.header.clone();
        if !parsed.certificates.is_empty() {
            header.insert("certificates", Value::Array(parsed.certificates.clone()));
        }
        let payload = parsed.payload.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut output = tempfile::NamedTempFile::new_in(
                journal
                    .path
                    .parent()
                    .ok_or_else(|| std::io::Error::other("quarantine file missing parent"))?,
            )?;
            let header = json::write_compact(&header.into());
            if header.len().saturating_add(1).saturating_add(payload.len()) as u64
                > super::MAX_JOURNALED_BYTES
            {
                return Err(std::io::Error::other(
                    "replay checkpoint exceeds quarantine file size limit",
                ));
            }
            writeln!(output, "{header}")?;
            output.write_all(&payload)?;
            output.as_file().sync_all()?;
            output.persist(&journal.path)?;
            journal.directory.as_ref().sync_all()
        })
        .await
        .map_err(std::io::Error::other)??;
        Ok(())
    }

    // Each normal part appends two small synchronized records. A refusal folds
    // the validated prefix back into the header once for inspection by the operator.
    async fn append(self: &Arc<Self>, entry: Object) -> Result<(), Error> {
        let journal = self.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let change: Value = entry.into();
            let digest = super::checkpoint_digest(&change);
            let mut line = json::write_compact(
                &Object::new()
                    .with("digest", digest)
                    .with("change", change)
                    .into(),
            );
            line.push('\n');
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&journal.path)?;
            if file.metadata()?.len().saturating_add(line.len() as u64) > super::MAX_JOURNALED_BYTES
            {
                return Err(std::io::Error::other(
                    "replay checkpoint exceeds quarantine file size limit",
                ));
            }
            file.write_all(line.as_bytes())?;
            file.sync_data()
        })
        .await
        .map_err(std::io::Error::other)??;
        Ok(())
    }

    pub(super) async fn reconcile(
        self: &Arc<Self>,
        client: &Client,
        file: &mut Parsed,
    ) -> Result<ArtifactReceipt, Error> {
        let info = client.discovery();
        let ambiguous = match file.header.get("original").and_then(Value::as_str) {
            Some("unsent" | "refused") => false,
            Some("submitted") => true,
            _ => return Err(corrupt("invalid original submission state")),
        };
        match file.header.get("store") {
            Some(Value::Null) if !ambiguous => {
                file.header.insert("store", info.store_id.clone());
            }
            Some(Value::String(store)) if store == &info.store_id => (),
            Some(Value::String(_)) => {
                return Err(Error::InvalidArgument(
                    "replay requires the file's original store",
                ));
            }
            _ => return Err(corrupt("missing original destination")),
        }
        let needs_binding = !ambiguous && file.header.get("part_events").is_none();
        let limits = info.event_limits();
        let parts = if ambiguous {
            vec![file.batch.clone()]
        } else {
            let (count, bytes) = match (
                file.header.get("part_events"),
                file.header.get("part_bytes"),
            ) {
                (None, None) => {
                    let bytes = limits.bytes.min(limits.payload_bytes);
                    file.header.insert("part_events", limits.events as u64);
                    file.header.insert("part_bytes", bytes as u64);
                    (limits.events, bytes)
                }
                (Some(count), Some(bytes)) => {
                    let bound = |n: &Value, max: usize| {
                        n.as_u64()
                            .and_then(|n| usize::try_from(n).ok())
                            .filter(|n| (1..=max).contains(n))
                            .ok_or_else(|| corrupt("invalid replay part ceiling"))
                    };
                    (
                        bound(count, crate::MAX_EVENT_BATCH_EVENTS)?,
                        bound(bytes, crate::MAX_EVENT_BATCH_BYTES)?,
                    )
                }
                _ => return Err(corrupt("incomplete replay part ceiling")),
            };
            replay_parts(&file.batch, count, bytes)?
        };
        if file.certificates.len() > parts.len()
            || (file.certificates.len() == parts.len()
                && file.header.get("replay_attempt").is_some())
        {
            return Err(corrupt("invalid certified prefix"));
        }
        if needs_binding {
            self.save(file).await?;
        }
        let mut last = None;
        for (index, part) in parts.iter().enumerate() {
            if let Some(certificate) = file.certificates.get(index) {
                last = Some(certified(client, part, certificate)?);
                continue;
            }
            let newly_attempted = !ambiguous && file.header.get("replay_attempt").is_none();
            // A reduced ceiling can refuse an indivisible event locally. Do not
            // create a deadline for a part that cannot leave this process.
            let bytes = if newly_attempted {
                part.source_bytes_with_limits(info.event_limits())?
            } else {
                part.source_bytes()?
            };
            let expected = ExpectedArtifact::bytes(
                info,
                &part.source,
                Format::Jsonl,
                &bytes,
                Durability::Durable,
                Some(part.events.len()),
            );
            let deadline = match file.header.get("replay_attempt") {
                Some(attempt) => {
                    if attempt.get("request").and_then(Value::as_str) != Some(&part.request_id)
                        || attempt.get("payload").and_then(Value::as_str)
                            != Some(&expected.payload_id)
                    {
                        return Err(corrupt(
                            "checkpoint does not name the exact next replay part",
                        ));
                    }
                    attempt
                        .get("deadline")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| corrupt("invalid replay deadline"))?
                }
                None => {
                    let deadline = if ambiguous {
                        0
                    } else {
                        let sample = Contract::Producer.discover(client.capability()).await?;
                        if sample.store_id != info.store_id {
                            return Err(Error::InvalidArgument("replay connection changed store"));
                        }
                        sample.sealed_event_deadline_ms
                    };
                    if !ambiguous && deadline == 0 {
                        return Err(Error::Unsupported(
                            "quarantine replay requires conditional sealed event ingestion",
                        ));
                    }
                    let attempt = Object::new()
                        .with("request", part.request_id.clone())
                        .with("payload", expected.payload_id)
                        .with("deadline", deadline);
                    self.append(
                        Object::new()
                            .with("part", index as u64)
                            .with("attempt", attempt.clone()),
                    )
                    .await?;
                    file.header.insert("replay_attempt", attempt);
                    deadline
                }
            };
            if ambiguous && deadline != 0 {
                return Err(corrupt("unguarded original requires a receipt-only probe"));
            }
            let response = client
                .submit_events_receipt(part, Durability::Durable, Some(deadline), |receipt, raw| {
                    let mut msg = capnp::message::Builder::new_default();
                    msg.set_root::<artifact::Owned>(raw)?;
                    let bytes = capnp::serialize::write_message_to_words(&msg);
                    if bytes.len() > 2048 + part.events.len() * 192 {
                        return Err(Error::Protocol(
                            "quarantine receipt exceeds checkpoint bound",
                        ));
                    }
                    Ok((receipt, purrdf_hash::hex::encode(&bytes)))
                })
                .await;
            let (receipt, certificate) = match response {
                Ok(result) => result,
                Err(error) => {
                    if newly_attempted && crate::quarantine::Original::definitively_refused(&error)
                    {
                        // The first attempt is known uncommitted. Recording the refusal
                        // atomically clears its seal so a later run can take a new deadline.
                        file.header.remove("replay_attempt");
                    }
                    return Err(error);
                }
            };
            self.append(
                Object::new()
                    .with("part", index as u64)
                    .with("receipt", certificate.clone()),
            )
            .await?;
            file.certificates.push(certificate.into());
            file.header.remove("replay_attempt");
            last = Some(receipt);
        }
        last.ok_or_else(|| corrupt("no replayable events"))
    }
}

/// Decode the exact server reply saved for this part and recheck its canonical bytes.
fn certified(client: &Client, part: &EventBatch, value: &Value) -> Result<ArtifactReceipt, Error> {
    let encoded = value
        .as_str()
        .ok_or_else(|| corrupt("invalid receipt checkpoint"))?;
    let bytes = purrdf_hash::hex::decode_canonical(encoded).map_err(corrupt)?;
    let mut input = bytes.as_slice();
    let message = capnp::serialize::read_message(&mut input, capnp::message::ReaderOptions::new())?;
    if !input.is_empty() {
        return Err(corrupt("trailing bytes in receipt"));
    }
    let receipt = ArtifactReceipt::decode(message.get_root::<artifact::Reader<'_>>()?)?;
    ExpectedArtifact::bytes(
        client.discovery(),
        &part.source,
        Format::Jsonl,
        &part.source_bytes()?,
        Durability::Durable,
        Some(part.events.len()),
    )
    .certify(receipt)
}
