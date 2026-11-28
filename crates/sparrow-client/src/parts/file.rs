// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Durable receipts for an ordered set read from a caller-owned completed file.

use super::{
    PartInput, PartLimits, PartSpan, is_one_part, manifest_body, manifest_part, next_cut,
    part_request,
};
use crate::{
    ArtifactReceipt, Client, CompletedFileIdentity, Durability, Error, ExclusiveFileLock, Expect,
    Format, bulk, format_label,
    import_progress::read_import_progress,
    imports::{self, Checkpoint, CompletedInput, StagedInput},
    parse_format,
    receipt::ExpectedArtifact,
};
use purrdf_lex::json;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::HashSet, fs::File, io, path::Path};

/// The largest flat manifest the access client reads and verifies.
pub const FILE_SET_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

/// The ordinary artifact or the durable manifest and ordered members of a completed file.
#[derive(Debug)]
pub enum FileImport {
    /// An ordinary artifact receipt.
    Single(ArtifactReceipt),
    /// The manifest receipt and member ranges in byte order.
    Set(ArtifactReceipt, Vec<(PartSpan, String)>),
}

fn key(request: &str) -> String {
    purrdf_hash::hex::encode(purrdf_hash::blake3::hash(request.as_bytes()).as_bytes())
}

/// Import a completed file intact or publish it as a durable ordered part set.
/// Resume an existing set from the per-store journal even if the advertised limits change.
/// The caller retains ownership of the original file.
pub async fn import_file_or_set(
    client: &Client,
    request: &str,
    source: &str,
    format: Format,
    path: &Path,
) -> Result<FileImport, Error> {
    let file = crate::open_completed_file(path)?;
    let input = crate::imports::staged_completed(&file, None).await?;
    let expect = Expect::store(&client.discovery().store_id);
    let endpoint = client.endpoint();
    let client = imports::reconnect(endpoint, Some(&expect)).await?;
    let discovery = client.discovery();
    let directory = crate::quarantine::state_directory()
        .map(|dir| dir.join("file-sets").join(key(&discovery.store_id)));
    let resume = match &directory {
        Some(directory) => directory.join(key(request)).join("intent").try_exists()?,
        None => false,
    };
    let length = input.length();
    let packet_limit = sparrow_limits::SYSLOG_MESSAGE_BYTES as u64;
    let limits = PartLimits::from_discovery(discovery, packet_limit);
    let split = match &limits {
        Ok(limits) => resume || !is_one_part(&file, length, format, limits)?,
        Err(_) => {
            resume
                || (discovery.max_import_bytes != 0 && length > discovery.max_import_bytes)
                || (format == Format::Syslog && length > packet_limit)
        }
    };
    input.unchanged()?;
    if !split {
        let receipt = imports::import_open_checked(
            endpoint,
            Some(&expect),
            request,
            source,
            format,
            &input,
            Some(client),
        )
        .await?;
        return Ok(FileImport::Single(receipt));
    }
    let limits = limits.map_err(|missing| Error::Domain {
        code: "UNSUPPORTED".into(),
        message: format!(
            "ordered file import requires daemon discovery to advertise {}",
            missing.names()
        ),
        retryable: false,
        class: crate::FailureClass::Unclassified,
    })?;
    let directory = directory.ok_or(Error::InvalidArgument(
        "ordered file import requires an absolute XDG_STATE_HOME or HOME for its receipt journal",
    ))?;
    let (manifest, parts) = publish_file_set(
        &client, &directory, request, source, format, &input, &limits,
    )
    .await?;
    Ok(FileImport::Set(manifest, parts))
}

fn read<T: DeserializeOwned>(dir: &Path, name: &str) -> Result<Option<T>, Error> {
    match bulk::read_meta(&dir.join(name)) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Intent {
    store: String,
    request: String,
    source: String,
    format: String,
    file: CompletedFileIdentity,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    start: u64,
    length: u64,
    format: String,
    artifact: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Publishing {
    request: String,
    import: String,
    digest: String,
}

// The remaining manifest receipt fields are fixed by its certified bytes and durable JSON
// publication. Keep the digest so a changed member ledger cannot reuse the certificate.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestReceipt {
    artifact: String,
    received_at: u64,
    expires_at: u64,
    digest: String,
    records: Vec<String>,
}

enum Publication {
    Part,
    Manifest,
}

struct Submission<'a, T: ?Sized> {
    bytes: &'a T,
    dir: &'a Path,
    request: &'a str,
    publication: Publication,
    expected: ExpectedArtifact<'a>,
}

impl<T: StagedInput + ?Sized> StagedInput for Submission<'_, T> {
    fn length(&self) -> u64 {
        self.bytes.length()
    }

    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error> {
        self.bytes.read_at(buffer, offset)
    }

    fn unchanged(&self) -> Result<(), Error> {
        self.bytes.unchanged()
    }

    fn payload_hint(&self) -> Option<&str> {
        Some(&self.expected.payload_id)
    }

    fn before_publish(&self, store: &str, import: &str) -> Result<(), Error> {
        if store != self.expected.discovery.store_id {
            return Err(Error::Protocol("file-set publication changed store"));
        }
        bulk::write_meta(
            self.dir,
            "publishing",
            &Publishing {
                request: self.request.into(),
                import: import.into(),
                digest: self.expected.payload_id.clone(),
            },
        )?;
        Ok(())
    }
}

impl<T: StagedInput + ?Sized> Submission<'_, T> {
    async fn send(&self, client: &Client) -> Result<ArtifactReceipt, Error> {
        self.unchanged()?;
        let store = &client.discovery().store_id;
        let mut checkpoint = Checkpoint::default();
        if let Some(ticket) = read::<Publishing>(self.dir, "publishing")?
            && ticket.request == self.request
        {
            if ticket.digest != self.expected.payload_id {
                return Err(Error::InvalidArgument(
                    "file-set bytes changed after preparation",
                ));
            }
            // A lost reply retries the native publication before sending more bytes.
            match imports::publish(client.capability(), store, &ticket.import, &self.expected).await
            {
                Ok(receipt) => {
                    self.unchanged()?;
                    return Ok(receipt);
                }
                Err(error)
                    if matches!(self.publication, Publication::Part)
                        && matches!(&error, Error::Domain { code, .. } if code == "NOT_FOUND") =>
                {
                    // Publication can also report a missing private record. A read of the
                    // staging identity distinguishes that failure from a reclaimed attempt.
                    match read_import_progress(client.capability(), store, &ticket.import).await {
                        Err(Error::Domain { code, .. }) if code == "NOT_FOUND" => {
                            checkpoint = Checkpoint::expecting_import(ticket.import);
                        }
                        Ok(_) => return Err(error),
                        Err(refusal) => return Err(refusal),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        imports::upload(
            client.capability(),
            client.discovery(),
            self.request,
            self.expected.source,
            self.expected.format,
            self,
            &mut checkpoint,
        )
        .await
    }

    async fn publish(&self, client: &Client) -> Result<ArtifactReceipt, Error> {
        match self.send(client).await {
            Err(Error::Transport(error)) if error.kind == capnp::ErrorKind::Disconnected => {
                let replacement = imports::reconnect(
                    client.endpoint(),
                    Some(&Expect::store(&self.expected.discovery.store_id)),
                )
                .await?;
                self.send(&replacement).await
            }
            result => result,
        }
    }
}

/// Publish an ordered set from the same open descriptor used to measure the file. `directory`
/// holds private, persistent metadata scoped to one store. Reissue the same request with the
/// original file to resume. Every completed part keeps its certified occurrence, and a lost
/// publication reply retries the original native import within the daemon's retry window.
/// The final durable JSON manifest uses `request` itself. Callers receive its receipt and
/// its members in byte order. The file remains caller-owned throughout.
async fn publish_file_set(
    client: &Client,
    directory: &Path,
    request: &str,
    source: &str,
    format: Format,
    input: &CompletedInput<'_>,
    limits: &PartLimits,
) -> Result<(ArtifactReceipt, Vec<(PartSpan, String)>), Error> {
    let CompletedInput { file, identity, .. } = *input;
    if !directory.is_absolute()
        || request.is_empty()
        || request.len() > 128
        || source.is_empty()
        || source.len() > 512
    {
        return Err(Error::InvalidArgument(
            "file set requires an absolute state directory and bounded nonempty request and source labels",
        ));
    }
    if identity.length == 0 {
        return Err(Error::InvalidArgument(
            "file set requires a nonempty completed regular file",
        ));
    }
    identity.require(file)?;
    let hash = key(request);
    let set = format!("fileset:{hash}");
    let dir = directory.join(hash);
    crate::quarantine::create_dir(&dir)?;
    let mut lock = ExclusiveFileLock::acquire(File::open(&dir)?)?;
    // A rename could have become visible just before an earlier directory sync failed.
    lock.as_ref().sync_all()?;
    let intent = Intent {
        store: client.discovery().store_id.clone(),
        request: request.into(),
        source: source.into(),
        format: format_label(format).into(),
        file: identity,
    };
    if let Some(saved) = read::<Intent>(&dir, "intent")? {
        Expect::store(&saved.store).require(client.discovery())?;
        if saved != intent {
            return Err(Error::InvalidArgument(
                "file-set request belongs to another input generation, source or format; retry with the original input",
            ));
        }
    } else {
        bulk::write_meta(&dir, "intent", &intent)?;
    }

    let complete = read::<ManifestReceipt>(&dir, "complete")?;
    let mut parts = Vec::new();
    let mut seen = HashSet::new();
    let mut start = 0;
    let mut manifest_length = manifest_body(&set, source, format, identity.length, &[]).len();
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    while start < identity.length {
        let index = u32::try_from(parts.len())
            .map_err(|_| Error::InvalidArgument("file-set ordinal exceeds its wire range"))?;
        let name = format!("part{index:010}");
        let mut member = match read::<Member>(&dir, &name)? {
            Some(member) => member,
            None if complete.is_some() => {
                return Err(Error::Protocol(
                    "completed file-set journal is missing a member",
                ));
            }
            None => {
                identity.require(file)?;
                let span = next_cut(file, identity.length, start, index, format, limits)?;
                let member = Member {
                    start,
                    length: span.length,
                    format: format_label(span.format).into(),
                    artifact: None,
                };
                bulk::write_meta(&dir, &name, &member)?;
                member
            }
        };
        let end = start
            .checked_add(member.length)
            .filter(|&end| end <= identity.length);
        let Some(end) = end.filter(|_| member.start == start && member.length > 0) else {
            return Err(Error::Protocol(
                "file-set journal has a gap or invalid byte range",
            ));
        };
        let span = PartSpan {
            index,
            start,
            length: member.length,
            format: parse_format(&member.format)?,
        };
        let id = match member.artifact.as_ref() {
            Some(id) => id.clone(),
            None if complete.is_some() => {
                return Err(Error::Protocol(
                    "completed file-set journal is missing a member receipt",
                ));
            }
            None => {
                let input = PartInput {
                    file,
                    identity,
                    start,
                    length: span.length,
                    payload: None,
                };
                let expected = ExpectedArtifact {
                    discovery: client.discovery(),
                    source,
                    format: span.format,
                    byte_length: span.length,
                    payload_id: imports::payload_id(&input, &mut buffer)?,
                    durability: Durability::Durable,
                    record_count: None,
                };
                let receipt = Submission {
                    bytes: &input,
                    dir: &dir,
                    request: &part_request(&set, index),
                    publication: Publication::Part,
                    expected,
                }
                .publish(client)
                .await?;
                member.artifact = Some(receipt.id.clone());
                bulk::write_meta(&dir, &name, &member)?;
                receipt.id
            }
        };
        if !id.starts_with("artifact:")
            || !(10..=128).contains(&id.len())
            || !seen.insert(id.clone())
        {
            return Err(Error::Protocol(
                "file-set journal contains an invalid or repeated artifact identity",
            ));
        }
        manifest_length +=
            usize::from(index != 0) + json::write_compact(&manifest_part(&span, &id)).len();
        let length = manifest_length as u64;
        if manifest_length > FILE_SET_MANIFEST_BYTES
            || (complete.is_none()
                && (length > limits.max_bytes.min(limits.raw_split_bytes)
                    || !limits.fits(length.div_ceil(super::CHUNK), 1)))
        {
            return Err(Error::InvalidArgument(
                "file-set manifest exceeds the applicable JSON artifact bound; split the file into smaller sets",
            ));
        }
        parts.push((span, id));
        start = end;
    }
    identity.require(file)?;
    let body = manifest_body(&set, source, format, identity.length, &parts);
    let expected = ExpectedArtifact::bytes(
        client.discovery(),
        source,
        Format::Json,
        &body,
        Durability::Durable,
        Some(1),
    );
    let receipt = if let Some(saved) = complete {
        let receipt = ArtifactReceipt {
            buffered_through: None,
            store_id: intent.store,
            id: saved.artifact,
            source: source.into(),
            format: Format::Json,
            received_at: saved.received_at,
            expires_at: saved.expires_at,
            byte_length: body.len() as u64,
            payload_id: saved.digest,
            record_ids: saved.records,
            durability: Durability::Durable,
        };
        receipt.validate()?;
        expected.certify(receipt)?
    } else {
        let receipt = Submission {
            bytes: body.as_slice(),
            dir: &dir,
            request,
            publication: Publication::Manifest,
            expected,
        }
        .publish(client)
        .await?;
        bulk::write_meta(
            &dir,
            "complete",
            &ManifestReceipt {
                artifact: receipt.id.clone(),
                received_at: receipt.received_at,
                expires_at: receipt.expires_at,
                digest: receipt.payload_id.clone(),
                records: receipt.record_ids.clone(),
            },
        )?;
        receipt
    };
    identity.require(file)?;
    lock.release()?;
    Ok((receipt, parts))
}
