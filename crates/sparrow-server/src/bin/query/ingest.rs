// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{failure, instant, invalid, wire};
use purrdf_lex::json::{Object, Value};
use std::{fs::File, io::Read, path::PathBuf};

pub(crate) struct Input {
    request: String,
    source: String,
    format: wire::Format,
    path: PathBuf,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> capnp::Result<Self> {
        if args.len() != 4 {
            return Err(invalid("usage: ingest REQUEST_ID SOURCE FORMAT FILE"));
        }
        let format = match args[2].as_str() {
            "jsonl" => wire::Format::Jsonl,
            "log" => wire::Format::Log,
            "json" => wire::Format::Json,
            "markdown" => wire::Format::Markdown,
            "opaque" => wire::Format::Opaque,
            _ => {
                return Err(invalid(
                    "format requires jsonl, log, json, markdown or opaque",
                ));
            }
        };
        Ok(Self {
            request: args[0].clone(),
            source: args[1].clone(),
            format,
            path: args[3].clone().into(),
        })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> capnp::Result<Value> {
        let response = client.status_request().send().promise.await?;
        let status = match response.get()?.get_result()?.which()? {
            wire::result::Ok(status) => status?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let limit = status.get_max_artifact_bytes();
        let file = File::open(&self.path)
            .map_err(|e| invalid(format!("read {}: {e}", self.path.display())))?;
        if !file
            .metadata()
            .map_err(|e| invalid(e.to_string()))?
            .is_file()
        {
            return Err(invalid(
                "ingest requires a regular completed file; live tailing is a separate operation",
            ));
        }
        let buffered_limit = limit.min(64 * 1024 * 1024);
        let mut bytes = Vec::new();
        file.take(buffered_limit + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| invalid(format!("read {}: {e}", self.path.display())))?;
        if bytes.len() as u64 > buffered_limit {
            return Err(invalid(format!(
                "RESULT_TOO_LARGE: file exceeds buffered import limit={buffered_limit} (daemon max_artifact_bytes={limit}); no ingest request sent"
            )));
        }
        let mut request = client.ingest_request();
        let mut input = request.get().init_request();
        input.set_request_id(&self.request);
        input.set_source(&self.source);
        input.set_format(self.format);
        input.set_bytes(&bytes);
        let response = request.send().promise.await?;
        let artifact = match response.get()?.get_result()?.which()? {
            wire::result::Ok(artifact) => artifact?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let records = artifact
            .get_record_ids()?
            .iter()
            .map(|id| Ok(Value::from(id?.to_str()?)))
            .collect::<capnp::Result<Vec<_>>>()?;
        Ok(Object::new()
            .with("store_id", artifact.get_store_id()?.to_str()?)
            .with("artifact_id", artifact.get_id()?.to_str()?)
            .with("source", artifact.get_source()?.to_str()?)
            .with(
                "format",
                match artifact.get_format()? {
                    wire::Format::Jsonl => "jsonl",
                    wire::Format::Log => "log",
                    wire::Format::Json => "json",
                    wire::Format::Markdown => "markdown",
                    wire::Format::Opaque => "opaque",
                },
            )
            .with("received_at", instant(artifact.get_received_at()?)?)
            .with("expires_at", instant(artifact.get_expires_at()?)?)
            .with("byte_length", artifact.get_byte_length().to_string())
            .with("payload_id", artifact.get_payload_id()?.to_str()?)
            .with("record_ids", Value::array(records))
            .with(
                "durability",
                match artifact.get_durability()? {
                    wire::Durability::Buffered => "buffered",
                    wire::Durability::Durable => "durable",
                },
            )
            .into())
    }
}
