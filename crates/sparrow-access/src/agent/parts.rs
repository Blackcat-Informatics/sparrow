// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Check each ordered part against the store's current artifact membership.

use super::{Result, artifacts, invalid, read, wire};
use purrdf_lex::json::{self, Object, Value, record::Record};
use sparrow_client::{Client, Expect, parts::FILE_SET_MANIFEST_BYTES};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct Verify {
    source: String,
    artifact: String,
}

impl Verify {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let [command, source, artifact] = args else {
            return Err(invalid("usage: parts verify SOURCE MANIFEST_ARTIFACT_ID"));
        };
        if command != "verify" || source.is_empty() || source.len() > 512 || !artifact_id(artifact)
        {
            return Err(invalid("usage: parts verify SOURCE MANIFEST_ARTIFACT_ID"));
        }
        Ok(Self {
            source: source.clone(),
            artifact: artifact.clone(),
        })
    }

    pub(super) async fn execute(&self, client: &Client) -> Result<Value> {
        let mut link = CatalogLink::new(client);
        let manifest = link
            .read(async |client| Ok(client.artifact(&self.artifact).await?))
            .await?;
        if manifest.source != self.source
            || manifest.format != wire::Format::Json
            || manifest.byte_length > FILE_SET_MANIFEST_BYTES as u64
        {
            return Err(invalid(
                "require a bounded JSON manifest in the named source",
            ));
        }
        let mut bytes = Vec::with_capacity(manifest.byte_length as usize);
        while (bytes.len() as u64) < manifest.byte_length {
            let start = bytes.len() as u64;
            let length = (manifest.byte_length - start).min(u64::from(read::MAX_BYTES)) as u32;
            let chunk = link
                .read(async |client| Ok(client.bytes(&self.artifact, start, length).await?))
                .await?;
            if chunk.len() != length as usize {
                return Err(invalid("part-set manifest read returned a short range"));
            }
            bytes.extend(chunk);
        }
        let parts = decode(&bytes, &self.source, &self.artifact)
            .map_err(|error| invalid(error.to_string()))?;
        let mut states = vec![Member::Missing; parts.len()];
        self.list_members(&mut link, &parts, &mut states).await?;
        let mut counts = [0_u64; 4];
        let mut rows = Vec::with_capacity(parts.len());
        for (part, state) in parts.into_iter().zip(states) {
            counts[state as usize] += 1;
            rows.push(Value::from(
                Object::new()
                    .with("index", part.index)
                    .with("artifact_id", part.id)
                    .with("status", state.label()),
            ));
        }
        let [available, missing, unavailable, mismatched] = counts;
        let complete = missing + unavailable + mismatched == 0;
        Ok(Object::new()
            .with("store_id", client.discovery().store_id.clone())
            .with("manifest_id", self.artifact.clone())
            .with("source", self.source.clone())
            .with(
                "execution_status",
                if complete { "complete" } else { "failed" },
            )
            .with("available", available)
            .with("missing", missing)
            .with("unavailable", unavailable)
            .with("mismatched", mismatched)
            .with("parts", Value::array(rows))
            .into())
    }

    async fn list_members(
        &self,
        link: &mut CatalogLink<'_>,
        parts: &[Part],
        states: &mut [Member],
    ) -> Result<()> {
        let mut remaining: BTreeMap<&str, usize> = parts
            .iter()
            .enumerate()
            .map(|(index, part)| (part.id.as_str(), index))
            .collect();
        let mut lease = None;
        let mut query = artifacts::Input {
            store: link.original.discovery().store_id.clone(),
            query: sparrow_core::ArtifactRead {
                selector: sparrow_core::ArtifactSelector {
                    source: Some(self.source.clone()),
                    ..Default::default()
                },
                ..Default::default()
            },
            selection: Box::default(),
        };
        let scanned: Result<()> = async {
            while !remaining.is_empty() {
                let (snapshot, page) = link
                    .read(async |client| {
                        query
                            .read_page(client.capability(), |page| {
                                let receipt = page.get_receipt()?;
                                let snapshot = receipt.get_snapshot_id()?.to_str()?.to_owned();
                                let content = (|| -> Result<_> {
                                    let next = match receipt.get_next_cursor()?.which()? {
                                        wire::optional_text::Absent(()) => None,
                                        wire::optional_text::Value(value) => {
                                            Some(value?.to_str()?.to_owned())
                                        }
                                    };
                                    let mut found = Vec::new();
                                    for row in page.get_rows()? {
                                        if let Some(&index) = remaining.get(row.get_id()?.to_str()?)
                                        {
                                            found.push((
                                                index,
                                                listed(row, &self.source, &parts[index])?,
                                            ));
                                        }
                                    }
                                    Ok((next, found))
                                })();
                                Ok((snapshot, content))
                            })
                            .await
                    })
                    .await?;
                lease = Some(snapshot);
                let (cursor, found) = page?;
                for (index, member) in found {
                    states[index] = member;
                    remaining.remove(parts[index].id.as_str());
                }
                let Some(cursor) = cursor else { break };
                query.query.cursor = Some(cursor);
            }
            Ok(())
        }
        .await;
        if let Some(snapshot) = lease {
            let release = link
                .read(async |client| Ok(client.release_result(&snapshot).await?))
                .await;
            scanned?;
            release?;
        } else {
            scanned?;
        }
        // An absent live-list row can still have retained origin metadata. Use it to
        // tell a retired member or a mismatched source from an unknown occurrence.
        for (id, index) in remaining {
            states[index] = match link
                .read(async |client| Ok(client.artifact(id).await?))
                .await
            {
                Ok(receipt)
                    if receipt.source != self.source
                        || receipt.format != parts[index].format
                        || receipt.byte_length != parts[index].length =>
                {
                    Member::Mismatched
                }
                Ok(_) => Member::Unavailable,
                Err(error) if error.code == "NOT_FOUND" => Member::Missing,
                Err(error) => return Err(error),
            };
        }
        Ok(())
    }
}

fn listed(row: wire::artifact_summary::Reader<'_>, source: &str, part: &Part) -> Result<Member> {
    if row.get_source()?.to_str()? != source {
        return Ok(Member::Mismatched);
    }
    match row.get_origin()?.which()? {
        wire::optional_artifact_origin::Value(origin) => {
            let origin = origin?;
            if origin.get_format()? != part.format || origin.get_byte_length() != part.length {
                return Ok(Member::Mismatched);
            }
        }
        wire::optional_artifact_origin::Absent(()) if row.get_complete_bytes_available() => {
            return Err(invalid("available part has no artifact origin"));
        }
        wire::optional_artifact_origin::Absent(()) => {}
    }
    Ok(if row.get_complete_bytes_available() {
        Member::Available
    } else {
        Member::Unavailable
    })
}

struct CatalogLink<'a> {
    original: &'a Client,
    replacement: Option<Client>,
}

impl<'a> CatalogLink<'a> {
    fn new(original: &'a Client) -> Self {
        Self {
            original,
            replacement: None,
        }
    }

    async fn read<T>(&mut self, read: impl AsyncFn(&Client) -> Result<T>) -> Result<T> {
        match read(self.replacement.as_ref().unwrap_or(self.original)).await {
            Err(error)
                if error.retryable
                    && matches!(error.code.as_str(), "RPC_FAILURE" | "SHUTTING_DOWN") =>
            {
                let client = Client::connect_endpoint_expecting(
                    self.original.endpoint(),
                    &Expect::store(&self.original.discovery().store_id),
                )
                .await?;
                read(self.replacement.insert(client)).await
            }
            result => result,
        }
    }
}

#[derive(Clone, Copy)]
enum Member {
    Available,
    Missing,
    Unavailable,
    Mismatched,
}

impl Member {
    const fn label(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Missing => "missing",
            Self::Unavailable => "unavailable",
            Self::Mismatched => "mismatched",
        }
    }
}

struct Part {
    index: u64,
    id: String,
    length: u64,
    format: wire::Format,
}

fn artifact_id(id: &str) -> bool {
    id.len() <= 128
        && id
            .strip_prefix("artifact:")
            .is_some_and(|name| !name.is_empty())
}

fn decode(
    bytes: &[u8],
    source: &str,
    manifest: &str,
) -> std::result::Result<Vec<Part>, json::record::DecodeError> {
    use json::record::DecodeError;
    let value = json::read_slice(
        bytes,
        json::Limits {
            max_string_bytes: 512,
            unique_members: true,
            max_depth: 4,
            max_values: bytes.len() as u64,
        },
    )
    .map_err(DecodeError::custom)?;
    let mut fields = Record::new(&value, "part-set manifest")?;
    let schema: String = fields.required("schema")?;
    let set: String = fields.required("set")?;
    let declared_source: String = fields.required("source")?;
    let format: String = fields.required("format")?;
    sparrow_client::parse_format(&format).map_err(DecodeError::custom)?;
    let byte_length: u64 = fields.required("byte_length")?;
    let entries: Vec<Value> = fields.required("parts")?;
    fields.deny_unknown()?;
    if schema != "sparrow.parts/1"
        || declared_source != source
        || set.is_empty()
        || set.len() > 128
        || entries.is_empty()
    {
        return Err(DecodeError::custom(
            "manifest requires a valid schema, source, set identity and at least one member",
        ));
    }
    let mut end = 0_u64;
    let mut seen = BTreeSet::new();
    let mut parts = Vec::with_capacity(entries.len());
    for (ordinal, entry) in entries.iter().enumerate() {
        let mut fields = Record::new(entry, "part-set member")?;
        let index = fields.required::<u64>("index")?;
        let start = fields.required::<u64>("start")?;
        let length = fields.required::<u64>("length")?;
        let format = fields.required::<String>("format")?;
        let id = fields.required::<String>("artifact")?;
        fields.deny_unknown()?;
        if index != ordinal as u64
            || start != end
            || length == 0
            || id == manifest
            || !artifact_id(&id)
            || !seen.insert(id.clone())
        {
            return Err(DecodeError::custom(
                "invalid part order, range or artifact identity",
            ));
        }
        end = end
            .checked_add(length)
            .ok_or_else(|| DecodeError::custom("part range overflows"))?;
        parts.push(Part {
            index,
            id,
            length,
            format: sparrow_client::parse_format(&format).map_err(DecodeError::custom)?,
        });
    }
    if end != byte_length {
        return Err(DecodeError::custom(
            "part ranges differ from the manifest byte length",
        ));
    }
    Ok(parts)
}
