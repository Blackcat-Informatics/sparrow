// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared CLI/MCP retained-generation completion over the typed capability.

use super::{Result, failure, invalid, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) enum Input {
    Prepare {
        request: String,
        store: String,
        generation: String,
        file: String,
        output: String,
    },
    Finish {
        intent: sparrow_client::TailFinishIntent,
        read_only: bool,
    },
}
impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        match args {
            [command, request, store, generation, file, output]
                if command == "file-finish-prepare" =>
            {
                Ok(Self::Prepare {
                    request: request.clone(),
                    store: store.clone(),
                    generation: generation.clone(),
                    file: file.clone(),
                    output: output.clone(),
                })
            }
            [command, file] if command == "file-finish" || command == "file-finish-status" => {
                Ok(Self::Finish {
                    intent: sparrow_client::TailFinishIntent::read(
                        sparrow_client::open_completed_file(std::path::Path::new(file))?,
                    )?,
                    read_only: command == "file-finish-status",
                })
            }
            _ => Err(invalid(
                "file-finish-prepare REQUEST_ID STORE_ID GENERATION_ID FINAL_FILE INTENT.capnp | file-finish INTENT.capnp | file-finish-status INTENT.capnp",
            )),
        }
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        match self {
            Self::Finish { intent, read_only } => {
                let receipt = sparrow_client::tail_finish(client, intent, *read_only).await?;
                Ok(Object::new()
                    .with("store_id", receipt.request.store_id)
                    .with("request_id", receipt.request.request_id)
                    .with("generation_id", receipt.request.generation_id)
                    .with("watch_revision", receipt.request.watch_revision.to_string())
                    .with(
                        "admitted_at",
                        sparrow_core::format_instant(receipt.admitted_at)
                            .map_err(|error| invalid(error.to_string()))?,
                    )
                    .with(
                        "finished_at",
                        receipt
                            .finished_at
                            .map(sparrow_core::format_instant)
                            .transpose()
                            .map_err(|error| invalid(error.to_string()))?,
                    )
                    .with("offset", receipt.offset.to_string())
                    .with("records", receipt.records.to_string())
                    .with("gap", receipt.gap)
                    .with("complete", receipt.finished_at.is_some())
                    .with("durability", "durable")
                    .into())
            }
            Self::Prepare {
                request,
                store,
                generation,
                file,
                output,
            } => {
                let discovery = sparrow_client::Discovery::fetch(client).await?;
                if discovery.store_id != *store {
                    return Err(invalid(
                        "completion preparation belongs to another persistent store",
                    ));
                }
                let mut call = client.tail_generation_get_request();
                let mut query = call.get().init_query();
                query.set_store_id(store);
                query.set_id(generation);
                query.set_schema_version(sparrow_client::SCHEMA_VERSION);
                let response = call.send().promise.await?;
                let row = match response.get()?.get_result()?.which()? {
                    wire::result::Ok(row) => row?,
                    wire::result::Error(error) => return Err(failure(error?)),
                };
                if row.get_complete()
                    || !matches!(row.get_gap()?.which()?, wire::optional_text::Absent(()))
                    || !matches!(
                        row.get_finish()?.which()?,
                        wire::optional_tail_finish::Absent(())
                    )
                {
                    return Err(invalid(
                        "prepare completion once for an active unbound generation; retry the saved original intent",
                    ));
                }
                let declared = row.get_declaration()?;
                let input = sparrow_client::open_completed_file(std::path::Path::new(file))?;
                let observed = sparrow_client::CompletedFileIdentity::capture(
                    &input.metadata().map_err(sparrow_client::Error::Io)?,
                );
                if (observed.device, observed.inode)
                    != (declared.get_device(), declared.get_inode())
                    || observed.length < row.get_offset()
                {
                    return Err(invalid(
                        "completion file does not match the original generation and checkpoint",
                    ));
                }
                let intent = sparrow_client::TailFinishIntent {
                    store_id: store.clone(),
                    request_id: request.clone(),
                    generation_id: generation.clone(),
                    watch_revision: declared.get_watch_revision(),
                    observed,
                };
                let response = client.watch_list_request().send().promise.await?;
                let list = match response.get()?.get_result()?.which()? {
                    wire::result::Ok(list) => list?,
                    wire::result::Error(error) => return Err(failure(error?)),
                };
                if list.get_store_id()?.to_str()? != *store {
                    return Err(invalid("watch list belongs to another persistent store"));
                }
                let parent = super::retry_file::parent(output, list)?;
                let mut bytes = Vec::new();
                intent.write(&mut bytes)?;
                observed.require(&input)?;
                super::retry_file::save(output, &parent, &bytes)?;
                Ok(Object::new()
                    .with("store_id", store.clone())
                    .with("request_id", request.clone())
                    .with("generation_id", generation.clone())
                    .with("watch_revision", intent.watch_revision.to_string())
                    .with("intent_path", output.clone())
                    .with("phase", "prepared")
                    .into())
            }
        }
    }
}
