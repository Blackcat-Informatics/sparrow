// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::Result;
use super::{invalid, wire};
use purrdf_lex::json::{Object, Value};
use std::{fs::OpenOptions, io::Write, path::PathBuf};

pub(super) const MAX_BYTES: u32 = 1024 * 1024;

pub(crate) struct Input {
    store: String,
    id: String,
    offset: u64,
    length: u32,
    path: PathBuf,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        if args.len() != 5 {
            return Err(invalid(
                "usage: bytes STORE_ID EVIDENCE_ID OFFSET LENGTH OUTPUT_FILE",
            ));
        }
        if !args[1].starts_with("artifact:")
            && !args[1].starts_with("record:")
            && !args[1].starts_with("file-generation:")
        {
            return Err(invalid(
                "byte target requires artifact:, record:, or file-generation: prefix",
            ));
        }
        let offset = args[2]
            .parse()
            .map_err(|_| invalid("offset requires UInt64"))?;
        let length = args[3]
            .parse()
            .map_err(|_| invalid("length requires UInt32"))?;
        if length > MAX_BYTES {
            return Err(invalid(
                "byte read limit is 1 MiB; select an explicit range",
            ));
        }
        Ok(Self {
            store: args[0].clone(),
            id: args[1].clone(),
            offset,
            length,
            path: args[4].clone().into(),
        })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let (store, data) = fetch(client, &self.store, &self.id, self.offset, self.length).await?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
            .map_err(|e| invalid(format!("create {}: {e}", self.path.display())))?;
        output
            .write_all(&data)
            .and_then(|()| output.sync_all())
            .map_err(|e| {
                invalid(format!(
                    "write {}: {e}; output may be incomplete",
                    self.path.display()
                ))
            })?;
        Ok(Object::new()
            .with("store_id", store)
            .with("evidence_id", self.id.clone())
            .with("offset", self.offset.to_string())
            .with("byte_length", data.len().to_string())
            .with("output_file", self.path.to_string_lossy().as_ref())
            .into())
    }
}

pub(super) async fn fetch(
    client: &wire::sparrow::Client,
    store: &str,
    id: &str,
    offset: u64,
    length: u32,
) -> Result<(String, Vec<u8>)> {
    sparrow_client::read_bytes(client, store, id, offset, length)
        .await
        .map_err(Into::into)
}
