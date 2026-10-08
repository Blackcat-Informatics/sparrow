// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Explicit CLI administration over generated watch requests and one renderer.

use super::{Result, decimal_u64, failure, invalid, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) enum Input {
    List,
    Put {
        request: String,
        expected: u64,
        definition: crate::ConsumeWatch,
    },
    Remove {
        request: String,
        id: String,
        expected: u64,
    },
    Prepare {
        request: String,
        id: String,
        expected: u64,
        relative: String,
        output: String,
        tail_generation_id: Option<String>,
    },
    Handoff {
        intent: sparrow_client::CompletedHandoff,
        read_only: bool,
    },
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        match args {
            [
                operation,
                request,
                id,
                expected,
                relative,
                output,
                tail @ ..,
            ] if operation == "handoff-prepare" && tail.len() <= 1 => {
                sparrow_core::validate_consumption_path(id, relative.as_bytes())
                    .map_err(|e| invalid(e.to_string()))?;
                Ok(Self::Prepare {
                    request: request.clone(),
                    id: id.clone(),
                    expected: decimal_u64(&Value::from(expected.clone()))
                        .map_err(|e| invalid(e.to_string()))?,
                    relative: relative.clone(),
                    output: output.clone(),
                    tail_generation_id: tail.first().cloned(),
                })
            }
            [operation, path] if operation == "handoff" || operation == "handoff-status" => {
                Ok(Self::Handoff {
                    intent: sparrow_client::CompletedHandoff::read(
                        sparrow_client::open_completed_file(std::path::Path::new(path))?,
                    )?,
                    read_only: operation == "handoff-status",
                })
            }
            [operation] if operation == "list" => Ok(Self::List),
            [operation, request, expected, path] if operation == "put" => {
                let text = std::fs::read_to_string(path).map_err(sparrow_client::Error::Io)?;
                let definition: crate::ConsumeWatch =
                    toml::from_str(&text).map_err(|error| invalid(error.to_string()))?;
                crate::ConsumeWatch::validate_fields(std::slice::from_ref(&definition))
                    .map_err(|error| invalid(error.to_string()))?;
                Ok(Self::Put {
                    request: request.clone(),
                    expected: decimal_u64(&Value::from(expected.clone()))
                        .map_err(|error| invalid(error.to_string()))?,
                    definition,
                })
            }
            [operation, request, id, expected] if operation == "remove" => Ok(Self::Remove {
                request: request.clone(),
                id: id.clone(),
                expected: decimal_u64(&Value::from(expected.clone()))
                    .map_err(|error| invalid(error.to_string()))?,
            }),
            _ => Err(invalid(
                "usage: watch list | watch put REQUEST_ID EXPECTED_REVISION DEFINITION.toml | watch remove REQUEST_ID WATCH_ID EXPECTED_REVISION | watch handoff-prepare REQUEST_ID WATCH_ID REVISION RELATIVE_PATH INTENT.capnp [TAIL_GENERATION_ID] | watch handoff INTENT.capnp | watch handoff-status INTENT.capnp; zero revision creates a watch",
            )),
        }
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let discovery = sparrow_client::Discovery::fetch(client).await?;
        match self {
            Self::Handoff { intent, read_only } => {
                let receipt = sparrow_client::completed_handoff(client, intent, *read_only).await?;
                Ok(Object::new()
                    .with("store_id", receipt.request.store_id)
                    .with("request_id", receipt.request.request_id)
                    .with("import_id", receipt.import_id)
                    .with(
                        "phase",
                        match receipt.phase {
                            wire::HandoffPhase::Reserved => "reserved",
                            wire::HandoffPhase::Claimed => "claimed",
                            wire::HandoffPhase::Published => "published",
                            wire::HandoffPhase::Removed => "removed",
                        },
                    )
                    .with("artifact_id", receipt.artifact_id)
                    .with("tail_generation_id", receipt.tail_generation_id)
                    .with("durability", "durable")
                    .into())
            }
            Self::Prepare {
                request,
                id,
                expected,
                relative,
                output,
                tail_generation_id,
            } => {
                let response = client.watch_list_request().send().promise.await?;
                let list = match response.get()?.get_result()?.which()? {
                    wire::result::Ok(value) => value?,
                    wire::result::Error(error) => return Err(failure(error?)),
                };
                if list.get_store_id()?.to_str()? != discovery.store_id {
                    return Err(invalid("watch list belongs to another persistent store"));
                }
                let parent = super::retry_file::parent(output, list)?;
                let mut root = None;
                for watch in list.get_watches()? {
                    let definition = watch.get_definition()?;
                    if definition.get_id()?.to_str()? == id {
                        if !watch.get_enabled() || watch.get_revision() != *expected {
                            return Err(invalid(
                                "handoff preparation requires the current enabled watch revision",
                            ));
                        }
                        if (definition.get_mode()? == wire::WatchMode::Tail)
                            != tail_generation_id.is_some()
                        {
                            return Err(invalid(
                                "tail handoff preparation requires the original completed generation ID",
                            ));
                        }
                        root = Some(std::path::PathBuf::from(definition.get_root()?.to_str()?));
                    }
                }
                let root = root.ok_or_else(|| invalid("handoff watch is not registered"))?;
                let file = sparrow_client::open_completed_file(&root.join(relative))?;
                let intent = sparrow_client::CompletedHandoff {
                    store_id: discovery.store_id,
                    request_id: request.clone(),
                    watch: id.clone(),
                    watch_revision: *expected,
                    relative_path: relative.as_bytes().to_vec(),
                    generation: sparrow_client::CompletedFileIdentity::capture(
                        &file.metadata().map_err(sparrow_client::Error::Io)?,
                    ),
                    tail_generation_id: tail_generation_id.clone(),
                };
                if let Some(generation) = tail_generation_id {
                    let mut call = client.tail_generation_get_request();
                    let mut query = call.get().init_query();
                    query.set_store_id(&intent.store_id);
                    query.set_id(generation);
                    query.set_schema_version(sparrow_client::SCHEMA_VERSION);
                    let response = call.send().promise.await?;
                    let row = match response.get()?.get_result()?.which()? {
                        wire::result::Ok(row) => row?,
                        wire::result::Error(error) => return Err(failure(error?)),
                    };
                    let finish = match row.get_finish()?.which()? {
                        wire::optional_tail_finish::Value(finish) => finish?,
                        wire::optional_tail_finish::Absent(()) => {
                            return Err(invalid(
                                "tail handoff requires the original producer finish",
                            ));
                        }
                    };
                    let declaration = row.get_declaration()?;
                    if row.get_store_id()?.to_str()? != intent.store_id
                        || row.get_id()?.to_str()? != generation
                        || !row.get_complete()
                        || !matches!(row.get_gap()?.which()?, wire::optional_text::Absent(()))
                        || !matches!(
                            finish.get_finished_at()?.which()?,
                            wire::optional_instant::Value(_)
                        )
                        || declaration.get_watch()?.to_str()? != id
                        || declaration.get_watch_revision() != *expected
                        || sparrow_client::CompletedFileIdentity::decode(
                            finish.get_request()?.get_observed()?,
                        )? != intent.generation
                    {
                        return Err(invalid(
                            "tail handoff preparation requires the original completed checkpoint and unchanged final file observation",
                        ));
                    }
                }
                // Encode before creating an output, then refuse overwriting an existing retry identity.
                let mut bytes = Vec::new();
                intent.write(&mut bytes)?;
                super::retry_file::save(output, &parent, &bytes)?;
                Ok(Object::new()
                    .with("store_id", intent.store_id)
                    .with("request_id", request.clone())
                    .with("watch", id.clone())
                    .with("watch_revision", expected.to_string())
                    .with("intent_path", output.clone())
                    .with("phase", "prepared")
                    .into())
            }
            Self::List => {
                let response = client.watch_list_request().send().promise.await?;
                let list = match response.get()?.get_result()?.which()? {
                    wire::result::Ok(value) => value?,
                    wire::result::Error(error) => return Err(failure(error?)),
                };
                let rows = list
                    .get_watches()?
                    .iter()
                    .map(info)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Object::new()
                    .with("store_id", list.get_store_id()?.to_str()?)
                    .with("watches", Value::from(rows))
                    .into())
            }
            Self::Put {
                request,
                expected,
                definition,
            } => {
                let mut call = client.watch_put_request();
                let mut input = call.get().init_request();
                input.set_store_id(&discovery.store_id);
                input.set_request_id(request);
                input.set_expected_revision(*expected);
                crate::watch_admin::encode_definition(input.init_definition(), definition)?;
                let response = call.send().promise.await?;
                mutation(response.get()?.get_result()?)
            }
            Self::Remove {
                request,
                id,
                expected,
            } => {
                let mut call = client.watch_remove_request();
                let mut input = call.get().init_request();
                input.set_store_id(&discovery.store_id);
                input.set_request_id(request);
                input.set_id(id);
                input.set_expected_revision(*expected);
                let response = call.send().promise.await?;
                mutation(response.get()?.get_result()?)
            }
        }
    }
}

fn mutation(result: wire::result::Reader<'_, wire::watch_mutation::Owned>) -> Result<Value> {
    let receipt = match result.which()? {
        wire::result::Ok(value) => value?,
        wire::result::Error(error) => return Err(failure(error?)),
    };
    if receipt.get_durability()? != wire::Durability::Durable {
        return Err(invalid(
            "watch mutation did not return a durable acknowledgment",
        ));
    }
    Ok(Object::new()
        .with("request_id", receipt.get_request_id()?.to_str()?)
        .with("watch", info(receipt.get_watch()?)?)
        .with("durability", "durable")
        .into())
}

fn info(value: wire::watch_info::Reader<'_>) -> Result<Value> {
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("revision", value.get_revision().to_string())
        .with("enabled", value.get_enabled())
        .with("definition", definition(value.get_definition()?)?)
        .into())
}

pub(super) fn definition(value: wire::watch_definition::Reader<'_>) -> Result<Value> {
    let definition =
        crate::watch_admin::decode_definition(value).map_err(|error| invalid(error.to_string()))?;
    Ok(Object::new()
        .with("id", definition.id)
        .with(
            "root",
            definition
                .root
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 watch root"))?,
        )
        .with("source", definition.source)
        .with("format", definition.format)
        .with("quiet_ms", u64::from(definition.quiet_ms))
        .with("max_directories", definition.max_directories as u64)
        .with("max_candidates", definition.max_candidates as u64)
        .with("recursive", true)
        .with(
            "mode",
            if definition.producer_contract == crate::ProducerContract::FinishedFiles {
                "consume"
            } else {
                "tail"
            },
        )
        .with("readiness", "close_or_idle")
        .with(
            "producer_contract",
            if definition.producer_contract == crate::ProducerContract::FinishedFiles {
                "finished_files"
            } else {
                "active_files_retain"
            },
        )
        .into())
}
