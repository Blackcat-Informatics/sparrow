// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

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
    /// Retire one unrecoverable claim of one watch; `store` is the caller's expected store,
    /// or the discovered one from the CLI.
    RetireClaim {
        store: Option<String>,
        watch: String,
        claim: String,
        reason: String,
    },
    /// Every pending claim, and every completed or abandoned claim that went pending within
    /// `since..=until` (Unix milliseconds; an absent end means now).
    Claims {
        since: u64,
        until: Option<u64>,
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
            [operation, options @ ..] if operation == "claims" => {
                let (mut since, mut until) = (0, None);
                for pair in options.chunks(2) {
                    let [flag, value] = pair else {
                        return Err(invalid("watch claims options take one value each"));
                    };
                    let at =
                        sparrow_core::parse_instant(value).map_err(|e| invalid(e.to_string()))?;
                    match flag.as_str() {
                        "--since" => since = at,
                        "--until" => until = Some(at),
                        _ => return Err(invalid(format!("unknown watch claims option: {flag}"))),
                    }
                }
                Ok(Self::Claims { since, until })
            }
            [operation, watch, claim, reason] if operation == "retire-claim" => {
                Ok(Self::RetireClaim {
                    store: None,
                    watch: watch.clone(),
                    claim: claim.clone(),
                    reason: reason.clone(),
                })
            }
            _ => Err(invalid(
                "usage: watch list | watch put REQUEST_ID EXPECTED_REVISION DEFINITION.toml | watch remove REQUEST_ID WATCH_ID EXPECTED_REVISION | watch handoff-prepare REQUEST_ID WATCH_ID REVISION RELATIVE_PATH INTENT.capnp [TAIL_GENERATION_ID] | watch handoff INTENT.capnp | watch handoff-status INTENT.capnp | watch retire-claim WATCH_ID CLAIM_ID REASON | watch claims [--since RFC3339] [--until RFC3339]; zero revision creates a watch",
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
            Self::Claims { since, until } => {
                let mut call = client.watch_claims_request();
                let mut query = call.get().init_query();
                query.set_store_id(&discovery.store_id);
                query.reborrow().init_since().set_unix_millis(*since);
                match until {
                    Some(at) => query.init_until().init_value().set_unix_millis(*at),
                    None => query.init_until().set_absent(()),
                }
                let response = call.send().promise.await?;
                match response.get()?.get_result()?.which()? {
                    wire::result::Ok(value) => claims(value?),
                    wire::result::Error(error) => Err(failure(error?)),
                }
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
            Self::RetireClaim {
                store,
                watch,
                claim,
                reason,
            } => {
                let mut call = client.watch_claim_retire_request();
                let mut input = call.get().init_request();
                input.set_store_id(store.as_deref().unwrap_or(&discovery.store_id));
                input.set_watch(watch);
                input.set_claim_id(claim);
                input.set_reason(reason);
                let response = call.send().promise.await?;
                let claim = match response.get()?.get_result()?.which()? {
                    wire::result::Ok(value) => value?,
                    wire::result::Error(error) => return Err(failure(error?)),
                };
                Ok(Object::new()
                    .with("store_id", discovery.store_id.as_str())
                    .with("watch", watch.as_str())
                    .with("retired_claim", retired_claim(claim)?)
                    .into())
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

fn clock(value: wire::optional_instant::Reader<'_>) -> Result<Option<u64>> {
    Ok(match value.which()? {
        wire::optional_instant::Value(at) => Some(at?.get_unix_millis()),
        wire::optional_instant::Absent(()) => None,
    })
}

fn clock_text(clock: Option<u64>) -> Result<Value> {
    clock.map_or(Ok(Value::Null), |at| {
        sparrow_core::format_instant(at)
            .map(Value::from)
            .map_err(|e| invalid(e.to_string()))
    })
}

/// A claim listing as JSON: per-state counts, then each claim with its history. A pending
/// claim's `age_ms` runs from its first-seen time to `as_of`.
fn claims(value: wire::watch_claims::Reader<'_>) -> Result<Value> {
    let as_of = value.get_as_of()?.get_unix_millis();
    let mut counts = [0_u64; 3];
    let mut rows = Vec::new();
    for claim in value.get_claims()? {
        let state = claim.get_state()?;
        let (index, name) = match state {
            wire::ClaimState::Pending => (0, "pending"),
            wire::ClaimState::Complete => (1, "complete"),
            wire::ClaimState::Abandoned => (2, "abandoned"),
        };
        counts[index] += 1;
        let first_seen = clock(claim.get_first_seen()?)?;
        let age = first_seen
            .filter(|_| state == wire::ClaimState::Pending)
            .map(|at| Value::from(as_of.saturating_sub(at).to_string()))
            .unwrap_or(Value::Null);
        let reason = match claim.get_last_pending_reason()?.which()? {
            wire::optional_text::Value(reason) => Value::from(reason?.to_str()?),
            wire::optional_text::Absent(()) => Value::Null,
        };
        rows.push(Value::from(
            Object::new()
                .with("claim_id", claim.get_claim_id()?.to_str()?)
                .with("watch", claim.get_watch()?.to_str()?)
                .with("state", name)
                .with(
                    "relative_path_hex",
                    purrdf_hash::hex::encode(claim.get_relative_path()?),
                )
                .with("first_seen", clock_text(first_seen)?)
                .with("age_ms", age)
                .with("attempts", u64::from(claim.get_attempts()))
                .with(
                    "last_attempt",
                    clock_text(clock(claim.get_last_attempt()?)?)?,
                )
                .with(
                    "last_pending_at",
                    clock_text(clock(claim.get_last_pending_at()?)?)?,
                )
                .with("last_pending_reason", reason)
                .with("finished_at", clock_text(clock(claim.get_finished_at()?)?)?),
        ));
    }
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("as_of", super::instant(value.get_as_of()?)?)
        .with("since", super::instant(value.get_since()?)?)
        .with("until", super::instant(value.get_until()?)?)
        .with("listed", rows.len() as u64)
        .with("pending", counts[0])
        .with("complete", counts[1])
        .with("abandoned", counts[2])
        .with("claims", Value::from(rows))
        .into())
}

/// One retired unrecoverable claim as JSON, shared by watch listings and status.
pub(super) fn retired_claim(value: wire::retired_claim::Reader<'_>) -> Result<Value> {
    Ok(Object::new()
        .with("claim_id", value.get_claim_id()?.to_str()?)
        .with("state", "retired-unrecoverable")
        .with(
            "relative_path_hex",
            purrdf_hash::hex::encode(value.get_relative_path()?),
        )
        .with("retired_at", super::instant(value.get_retired_at()?)?)
        .with("caller", value.get_caller()?.to_str()?)
        .with("reason", value.get_reason()?.to_str()?)
        .into())
}

/// The retired unrecoverable claims of one watch as a JSON list.
pub(super) fn retired_claims(
    claims: capnp::struct_list::Reader<'_, wire::retired_claim::Owned>,
) -> Result<Value> {
    Ok(Value::from(
        claims
            .iter()
            .map(retired_claim)
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn info(value: wire::watch_info::Reader<'_>) -> Result<Value> {
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("revision", value.get_revision().to_string())
        .with("enabled", value.get_enabled())
        .with(
            "retired_claims",
            retired_claims(value.get_retired_claims()?)?,
        )
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
