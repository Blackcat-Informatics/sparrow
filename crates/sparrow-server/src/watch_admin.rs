// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One durable watch catalog and finite administrative command channel.

use crate::{ConsumeWatch, consumption::Error};
use serde::{Deserialize, Serialize};
use sparrow_core::Store;
use tokio::sync::{mpsc, oneshot};

const MAX_RECEIPTS: usize = 128;
pub(crate) const MAX_WATCH_COMMANDS: usize = 16;
const RETRY_MS: u64 = 172_800_000;

/// Durable definition, revision and admission state of one watch.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedWatch {
    /// Complete explicit finished-file authority and bounds.
    pub(crate) definition: ConsumeWatch,
    /// Monotonic catalog revision used by administrative compare-and-set.
    pub(crate) revision: u64,
    /// New input admission; false retains only existing claim recovery.
    pub(crate) enabled: bool,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    request_id: String,
    fingerprint: String,
    expires_at: u64,
    watch: ManagedWatch,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    store_id: String,
    revision: u64,
    watches: Vec<ManagedWatch>,
    receipts: Vec<Receipt>,
}

pub(crate) struct Catalog {
    state: State,
    raw: Option<Vec<u8>>,
}

pub(crate) enum Action {
    Put {
        request_id: String,
        expected: u64,
        definition: ConsumeWatch,
    },
    Remove {
        request_id: String,
        expected: u64,
        id: String,
    },
}

pub(crate) enum Command {
    Mutation {
        action: Action,
        reply: oneshot::Sender<Result<ManagedWatch, sparrow_core::Error>>,
    },
    Handoff {
        intent: sparrow_core::ConsumptionHandoff,
        reply: oneshot::Sender<Result<sparrow_core::ConsumptionJournal, sparrow_core::Error>>,
    },
    TailFinish {
        intent: sparrow_core::TailFinishIntent,
        reply: oneshot::Sender<Result<sparrow_core::TailGeneration, sparrow_core::Error>>,
    },
    RetireClaim {
        retirement: ClaimRetirement,
        reply: oneshot::Sender<Result<sparrow_core::RetiredConsumption, sparrow_core::Error>>,
    },
}

/// An administrator's request to retire one unrecoverable claim of one watch.
pub(crate) struct ClaimRetirement {
    pub(crate) watch: String,
    pub(crate) claim: String,
    pub(crate) caller: String,
    pub(crate) reason: String,
}

/// Bounded typed control path to the one worker owning durable watch mutations.
#[derive(Clone)]
pub struct WatchController(mpsc::Sender<Command>);

pub(crate) fn channel() -> (WatchController, mpsc::Receiver<Command>) {
    let (sender, receiver) = mpsc::channel(MAX_WATCH_COMMANDS);
    (WatchController(sender), receiver)
}

pub(crate) fn failure(code: &'static str, message: impl ToString) -> sparrow_core::Error {
    sparrow_core::Error::new(code, message.to_string())
}

pub(crate) fn decode_definition(
    value: crate::sparrow_capnp::watch_definition::Reader<'_>,
) -> Result<ConsumeWatch, sparrow_core::Error> {
    use crate::sparrow_capnp as wire;
    let decode = || -> capnp::Result<ConsumeWatch> {
        if !value.get_recursive() || value.get_readiness()? != wire::WatchReadiness::CloseOrIdle {
            return Err(capnp::Error::failed(
                "only recursive watches with closeOrIdle readiness are implemented".into(),
            ));
        }
        let producer_contract = match (value.get_mode()?, value.get_authority()?.which()?) {
            (
                wire::WatchMode::Consume,
                wire::watch_authority::FinishedFilesDeleteAfterDurablePublish(()),
            ) => crate::ProducerContract::FinishedFiles,
            (wire::WatchMode::Tail, wire::watch_authority::ActiveFilesRetain(())) => {
                crate::ProducerContract::ActiveFilesRetain
            }
            _ => return Err(capnp::Error::failed(
                "watch mode requires its explicit matching producer contract and source authority"
                    .into(),
            )),
        };
        let id = value.get_id()?.to_str()?;
        let root = value.get_root()?.to_str()?;
        let source = value.get_source()?.to_str()?;
        if id.len() > 128 || root.len() > 2048 || source.len() > 512 {
            return Err(capnp::Error::failed(
                "watch labels exceed their bounded admission sizes".into(),
            ));
        }
        Ok(ConsumeWatch {
            id: id.into(),
            root: root.into(),
            source: source.into(),
            format: sparrow_client::format_label(value.get_format()?).into(),
            producer_contract,
            quiet_ms: value.get_quiet_ms(),
            max_directories: value.get_max_directories() as usize,
            max_candidates: value.get_max_candidates() as usize,
        })
    };
    let definition = decode().map_err(|error| failure("INVALID_ARGUMENT", error))?;
    ConsumeWatch::validate_fields(std::slice::from_ref(&definition))
        .map_err(|error| failure("INVALID_ARGUMENT", error))?;
    Ok(definition)
}

pub(crate) fn encode_definition(
    mut out: crate::sparrow_capnp::watch_definition::Builder<'_>,
    value: &ConsumeWatch,
) -> capnp::Result<()> {
    out.set_id(&value.id);
    out.set_root(value.root.to_str().ok_or_else(|| {
        capnp::Error::failed("configured watch root must have a UTF-8 representation".into())
    })?);
    out.set_source(&value.source);
    out.set_format(sparrow_client::parse_format(&value.format)?);
    out.set_quiet_ms(value.quiet_ms);
    out.set_max_directories(value.max_directories as u32);
    out.set_max_candidates(value.max_candidates as u32);
    out.set_recursive(true);
    out.set_readiness(crate::sparrow_capnp::WatchReadiness::CloseOrIdle);
    match value.producer_contract {
        crate::ProducerContract::FinishedFiles => {
            out.set_mode(crate::sparrow_capnp::WatchMode::Consume);
            out.init_authority()
                .set_finished_files_delete_after_durable_publish(());
        }
        crate::ProducerContract::ActiveFilesRetain => {
            out.set_mode(crate::sparrow_capnp::WatchMode::Tail);
            out.init_authority().set_active_files_retain(());
        }
    }
    Ok(())
}

pub(crate) fn encode_info(
    mut out: crate::sparrow_capnp::watch_info::Builder<'_>,
    store: &str,
    watch: &ManagedWatch,
    retired: &[sparrow_core::RetiredConsumption],
) -> capnp::Result<()> {
    out.set_store_id(store);
    out.set_revision(watch.revision);
    out.set_enabled(watch.enabled);
    encode_retired(
        |count| out.reborrow().init_retired_claims(count),
        &watch.definition.id,
        retired,
    );
    encode_definition(out.init_definition(), &watch.definition)
}

/// The claims of `watch` retired unrecoverable, in a list `init` sizes.
pub(crate) fn encode_retired<'a>(
    init: impl FnOnce(
        u32,
    )
        -> capnp::struct_list::Builder<'a, crate::sparrow_capnp::retired_claim::Owned>,
    watch: &str,
    retired: &[sparrow_core::RetiredConsumption],
) {
    let claims = retired
        .iter()
        .filter(|claim| claim.journal.watch == watch)
        .collect::<Vec<_>>();
    let mut out = init(claims.len() as u32);
    for (index, claim) in claims.into_iter().enumerate() {
        encode_retired_claim(out.reborrow().get(index as u32), claim);
    }
}

/// One retired unrecoverable claim on the wire.
pub(crate) fn encode_retired_claim(
    mut out: crate::sparrow_capnp::retired_claim::Builder<'_>,
    claim: &sparrow_core::RetiredConsumption,
) {
    out.set_claim_id(&claim.journal.import_id);
    out.set_relative_path(&claim.journal.relative_path);
    out.reborrow()
        .init_retired_at()
        .set_unix_millis(claim.retirement.retired_at);
    out.set_caller(&claim.retirement.caller);
    out.set_reason(&claim.retirement.reason);
}

impl WatchController {
    pub(crate) async fn tail_finish(
        &self,
        intent: sparrow_core::TailFinishIntent,
    ) -> Result<sparrow_core::TailGeneration, sparrow_core::Error> {
        self.enqueue(|reply| Command::TailFinish { intent, reply })
            .await
    }
    pub(crate) async fn submit(&self, action: Action) -> Result<ManagedWatch, sparrow_core::Error> {
        self.enqueue(|reply| Command::Mutation { action, reply })
            .await
    }

    pub(crate) async fn retire_claim(
        &self,
        retirement: ClaimRetirement,
    ) -> Result<sparrow_core::RetiredConsumption, sparrow_core::Error> {
        self.enqueue(|reply| Command::RetireClaim { retirement, reply })
            .await
    }

    pub(crate) async fn handoff(
        &self,
        intent: sparrow_core::ConsumptionHandoff,
    ) -> Result<sparrow_core::ConsumptionJournal, sparrow_core::Error> {
        self.enqueue(|reply| Command::Handoff { intent, reply })
            .await
    }

    async fn enqueue<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, sparrow_core::Error>>) -> Command,
    ) -> Result<T, sparrow_core::Error> {
        let (reply, result) = oneshot::channel();
        self.0
            .try_send(command(reply))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => failure(
                    "RESOURCE_LIMIT",
                    "watch administration queue is full; retry the original request identity",
                ),
                mpsc::error::TrySendError::Closed(_) => failure(
                    "STORAGE_FAILURE",
                    "watch administration worker stopped; inspect daemon state",
                ),
            })?;
        result.await.map_err(|_| {
            failure(
                "STORAGE_FAILURE",
                "watch mutation acknowledgment lost; retry its original identity",
            )
        })?
    }
}

impl Action {
    fn identity(&self) -> Result<(&str, String), sparrow_core::Error> {
        let mut framed = Vec::new();
        let (id, expected) = match self {
            Self::Put {
                request_id,
                expected,
                definition,
            } => {
                purrdf_hash::frame::frame_le(&mut framed, b"watch-put/1");
                let body = toml::to_string(definition)
                    .map_err(|error| failure("INVALID_ARGUMENT", error))?;
                purrdf_hash::frame::frame_le(&mut framed, body.as_bytes());
                (request_id, expected)
            }
            Self::Remove {
                request_id,
                expected,
                id,
            } => {
                purrdf_hash::frame::frame_le(&mut framed, b"watch-remove/1");
                purrdf_hash::frame::frame_le(&mut framed, id.as_bytes());
                (request_id, expected)
            }
        };
        if id.is_empty() || id.len() > 128 {
            return Err(failure(
                "INVALID_ARGUMENT",
                "watch mutation request identity requires 1..128 bytes",
            ));
        }
        purrdf_hash::frame::frame_le(&mut framed, &expected.to_le_bytes());
        Ok((
            id,
            purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&framed).as_bytes()),
        ))
    }
}

impl Catalog {
    pub(crate) fn load(store: &Store, bootstrap: &[ConsumeWatch]) -> Result<Self, Error> {
        let raw = store.watch_registry()?;
        let store_id = store.store_id()?;
        let state = if let Some(raw) = &raw {
            let text =
                std::str::from_utf8(raw).map_err(|error| failure("STORAGE_FAILURE", error))?;
            let state: State = toml::from_str(text).map_err(|error| {
                failure(
                    "STORAGE_FAILURE",
                    format!("invalid durable watch catalog: {error}"),
                )
            })?;
            if state.version != 1
                || state.store_id != store_id
                || state.watches.len() > crate::config::MAX_CONSUME_ROOTS
                || state.receipts.len() > MAX_RECEIPTS
            {
                return Err(failure(
                    "STORAGE_FAILURE",
                    "watch catalog version, store binding or capacity invariant failed",
                )
                .into());
            }
            let mut receipts = std::collections::BTreeSet::new();
            for receipt in &state.receipts {
                ConsumeWatch::validate_fields(std::slice::from_ref(&receipt.watch.definition))
                    .map_err(|error| failure("STORAGE_FAILURE", error))?;
                if receipt.request_id.is_empty()
                    || receipt.request_id.len() > 128
                    || !receipts.insert(&receipt.request_id)
                    || receipt.fingerprint.len() != 64
                    || !receipt
                        .fingerprint
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    || receipt.watch.revision == 0
                    || receipt.watch.revision > state.revision
                {
                    return Err(failure(
                        "STORAGE_FAILURE",
                        "invalid durable watch mutation receipt",
                    )
                    .into());
                }
            }
            if state
                .watches
                .iter()
                .any(|watch| watch.revision == 0 || watch.revision > state.revision)
            {
                return Err(failure(
                    "STORAGE_FAILURE",
                    "invalid durable watch definition revision",
                )
                .into());
            }
            state
        } else {
            State {
                version: 1,
                store_id,
                revision: bootstrap.len() as u64,
                watches: bootstrap
                    .iter()
                    .enumerate()
                    .map(|(index, definition)| ManagedWatch {
                        definition: definition.clone(),
                        revision: index as u64 + 1,
                        enabled: true,
                    })
                    .collect(),
                receipts: Vec::new(),
            }
        };
        Ok(Self { state, raw })
    }

    pub(crate) fn watches(&self) -> &[ManagedWatch] {
        &self.state.watches
    }
    pub(crate) fn needs_initial_commit(&self) -> bool {
        self.raw.is_none()
    }

    pub(crate) fn persist(&mut self, store: &Store) -> Result<(), Error> {
        let next = toml::to_string(&self.state)
            .map_err(|error| failure("STORAGE_FAILURE", error))?
            .into_bytes();
        store.watch_registry_replace(self.raw.as_deref(), &next)?;
        self.raw = Some(next);
        Ok(())
    }

    pub(crate) fn prepare(
        &self,
        action: &Action,
        now: u64,
    ) -> Result<(Self, ManagedWatch, bool), sparrow_core::Error> {
        let (request_id, fingerprint) = action.identity()?;
        if let Some(receipt) = self
            .state
            .receipts
            .iter()
            .find(|receipt| receipt.request_id == request_id)
        {
            if receipt.fingerprint != fingerprint {
                return Err(failure(
                    "CONFLICT",
                    "watch request identity was reused with different mutation arguments",
                ));
            }
            if receipt.expires_at <= now {
                return Err(failure(
                    "NOT_FOUND",
                    "watch mutation receipt expired; inspect current revisions before issuing a new request",
                ));
            }
            return Ok((
                Self {
                    state: self.state.clone(),
                    raw: self.raw.clone(),
                },
                receipt.watch.clone(),
                true,
            ));
        }
        let mut state = self.state.clone();
        state.receipts.retain(|receipt| receipt.expires_at > now);
        let receipt_limit = if matches!(action, Action::Put { .. }) {
            MAX_RECEIPTS - crate::config::MAX_CONSUME_ROOTS
        } else {
            MAX_RECEIPTS
        };
        if state.receipts.len() >= receipt_limit {
            return Err(failure(
                "RESOURCE_LIMIT",
                "watch mutation receipt capacity occupied; removal capacity is reserved separately; wait for expiry before a new mutation",
            ));
        }
        let (id, expected) = match action {
            Action::Put {
                definition,
                expected,
                ..
            } => (&definition.id, *expected),
            Action::Remove { id, expected, .. } => (id, *expected),
        };
        if id.is_empty() || id.len() > 128 {
            return Err(failure(
                "INVALID_ARGUMENT",
                "watch identity requires 1..128 bytes",
            ));
        }
        let previous = state
            .watches
            .iter()
            .find(|watch| watch.definition.id == *id);
        if previous.map_or(0, |watch| watch.revision) != expected {
            return Err(failure(
                "CONFLICT",
                "watch revision changed; inspect current state before retrying",
            ));
        }
        state.revision = state
            .revision
            .checked_add(1)
            .filter(|revision| *revision <= i64::MAX as u64)
            .ok_or_else(|| failure("RESOURCE_LIMIT", "watch catalog revision exhausted"))?;
        let watch = match action {
            Action::Put { definition, .. } => ManagedWatch {
                definition: definition.clone(),
                revision: state.revision,
                enabled: true,
            },
            Action::Remove { .. } => {
                let mut watch = previous
                    .ok_or_else(|| failure("NOT_FOUND", "unknown watch"))?
                    .clone();
                watch.revision = state.revision;
                watch.enabled = false;
                watch
            }
        };
        state
            .watches
            .retain(|previous| previous.definition.id != *id);
        state.watches.push(watch.clone());
        state.receipts.push(Receipt {
            request_id: request_id.into(),
            fingerprint,
            expires_at: now
                .checked_add(RETRY_MS)
                .ok_or_else(|| failure("INVALID_ARGUMENT", "watch receipt clock overflow"))?,
            watch: watch.clone(),
        });
        Ok((
            Self {
                state,
                raw: self.raw.clone(),
            },
            watch,
            false,
        ))
    }

    pub(crate) fn retire(&mut self, pending: &[sparrow_core::ConsumptionJournal]) -> bool {
        let before = self.state.watches.len();
        self.state.watches.retain(|watch| {
            watch.enabled
                || pending
                    .iter()
                    .any(|journal| journal.watch == watch.definition.id)
        });
        before != self.state.watches.len()
    }
}
