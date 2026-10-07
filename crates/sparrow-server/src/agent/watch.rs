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
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        match args {
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
                "usage: watch list | watch put REQUEST_ID EXPECTED_REVISION DEFINITION.toml | watch remove REQUEST_ID WATCH_ID EXPECTED_REVISION; zero revision creates a watch",
            )),
        }
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let discovery = sparrow_client::Discovery::fetch(client).await?;
        match self {
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
        .with("mode", "consume")
        .with("readiness", "close_or_idle")
        .with("producer_contract", "finished_files")
        .into())
}
