// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{failure, invalid, wire};
use purrdf_lex::json::{Object, Value};
use std::{fs::OpenOptions, io::Write, path::PathBuf};

pub(crate) struct Input {
    store: String,
    id: String,
    offset: u64,
    length: u32,
    path: PathBuf,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> capnp::Result<Self> {
        if args.len() != 5 {
            return Err(invalid(
                "usage: bytes STORE_ID EVIDENCE_ID OFFSET LENGTH OUTPUT_FILE",
            ));
        }
        if !args[1].starts_with("artifact:") && !args[1].starts_with("record:") {
            return Err(invalid("evidence ID requires artifact: or record: prefix"));
        }
        let offset = args[2]
            .parse()
            .map_err(|_| invalid("offset requires UInt64"))?;
        let length = args[3]
            .parse()
            .map_err(|_| invalid("length requires UInt32"))?;
        if length > 1048576 {
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

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> capnp::Result<Value> {
        let mut request = client.bytes_read_request();
        let mut read = request.get().init_request();
        read.set_offset(self.offset);
        read.set_length(self.length);
        let mut evidence = read.init_evidence();
        evidence.set_store_id(&self.store);
        if self.id.starts_with("artifact:") {
            evidence.set_artifact(&self.id);
        } else {
            evidence.set_record(&self.id);
        }
        let response = request.send().promise.await?;
        let bytes = match response.get()?.get_result()?.which()? {
            wire::result::Ok(bytes) => bytes?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let data = bytes.get_data()?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
            .map_err(|e| invalid(format!("create {}: {e}", self.path.display())))?;
        output
            .write_all(data)
            .and_then(|()| output.sync_all())
            .map_err(|e| {
                invalid(format!(
                    "write {}: {e}; output may be incomplete",
                    self.path.display()
                ))
            })?;
        Ok(Object::new()
            .with("store_id", bytes.get_store_id()?.to_str()?)
            .with("evidence_id", self.id.clone())
            .with("offset", self.offset.to_string())
            .with("byte_length", data.len().to_string())
            .with("output_file", self.path.to_string_lossy().as_ref())
            .into())
    }
}
