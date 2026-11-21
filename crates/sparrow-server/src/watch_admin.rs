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

/// What [`Catalog::retire_and_store`] did.
pub(crate) enum Retirement {
    Nothing,
    Stored,
    /// Pressure refused the write; the catalog is unchanged and the work stays due.
    Deferred(String),
}

#[derive(Clone)]
pub(crate) struct Catalog {
    state: State,
    raw: Option<Vec<u8>>,
    /// Under `sparrowd --private-intake`, each private root and the configured root it
    /// replaced; persistence writes the configured one, so a disposable copy never rewrites
    /// where its catalog's watches consume.
    configured: std::collections::BTreeMap<std::path::PathBuf, std::path::PathBuf>,
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
            publish_hold: value.get_publish_hold(),
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
    out.set_publish_hold(value.publish_hold);
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
        Ok(Self {
            state,
            raw,
            configured: std::collections::BTreeMap::new(),
        })
    }

    pub(crate) fn watches(&self) -> &[ManagedWatch] {
        &self.state.watches
    }
    /// Rebind every definition in memory, as `sparrowd --private-intake` does before any root
    /// opens; REBIND returns the configured root it replaced. A later mutation persists the
    /// configured roots, never the rebound ones. Two configured roots with one final name
    /// would share a private root and only one of them could be persisted, so that refuses
    /// the open and names both.
    pub(crate) fn rebind_roots(
        &mut self,
        mut rebind: impl FnMut(&mut ConsumeWatch) -> Result<std::path::PathBuf, Error>,
    ) -> Result<(), Error> {
        for watch in &mut self.state.watches {
            let configured = rebind(&mut watch.definition)?;
            let private = watch.definition.root.clone();
            if let Some(other) = self.configured.get(&private)
                && *other != configured
            {
                return Err(Error::PrivateRootShared {
                    first: other.clone(),
                    second: configured,
                    private,
                });
            }
            self.configured.insert(private, configured);
        }
        Ok(())
    }
    /// Refuse a put of watch ID whose rebound root PRIVATE another watch already holds for a
    /// different configured root: the two would consume one private root, and persisting
    /// would lose one configured root.
    pub(crate) fn admit_private_root(
        &self,
        id: &str,
        private: &std::path::Path,
        configured: &std::path::Path,
    ) -> Result<(), sparrow_core::Error> {
        let held = self
            .state
            .watches
            .iter()
            .any(|watch| watch.definition.id != id && watch.definition.root == private);
        match self.configured.get(private) {
            Some(other) if held && other != configured => Err(failure(
                "INVALID_ARGUMENT",
                Error::PrivateRootShared {
                    private: private.to_owned(),
                    first: other.clone(),
                    second: configured.to_owned(),
                },
            )),
            _ => Ok(()),
        }
    }
    /// Record that PRIVATE replaced CONFIGURED for a put; returns the previous record.
    pub(crate) fn remember_root(
        &mut self,
        private: std::path::PathBuf,
        configured: std::path::PathBuf,
    ) -> Option<std::path::PathBuf> {
        self.configured.insert(private, configured)
    }
    /// Put back the record [`Self::remember_root`] replaced, after a refused put.
    pub(crate) fn restore_root(
        &mut self,
        private: std::path::PathBuf,
        previous: Option<std::path::PathBuf>,
    ) {
        match previous {
            Some(configured) => self.configured.insert(private, configured),
            None => self.configured.remove(&private),
        };
    }
    pub(crate) fn needs_initial_commit(&self) -> bool {
        self.raw.is_none()
    }

    pub(crate) fn persist(&mut self, store: &Store) -> Result<(), Error> {
        let mut state = self.state.clone();
        let State {
            watches, receipts, ..
        } = &mut state;
        for definition in watches.iter_mut().map(|watch| &mut watch.definition).chain(
            receipts
                .iter_mut()
                .map(|receipt| &mut receipt.watch.definition),
        ) {
            if let Some(configured) = self.configured.get(&definition.root) {
                definition.root = configured.clone();
            }
        }
        let next = toml::to_string(&state)
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
                    configured: self.configured.clone(),
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
                configured: self.configured.clone(),
            },
            watch,
            false,
        ))
    }

    /// Drop the disabled watches that hold no unfinished journal and store the result with
    /// `persist`. The drop is made on a copy, so a write that pressure refuses leaves this
    /// catalog as stored and the next call finds the same work again.
    pub(crate) fn retire_and_store(
        &mut self,
        pending: &[sparrow_core::ConsumptionJournal],
        persist: impl FnOnce(&mut Self) -> Result<(), Error>,
    ) -> Result<Retirement, Error> {
        let mut next = self.clone();
        if !next.retire(pending) {
            return Ok(Retirement::Nothing);
        }
        match persist(&mut next) {
            Ok(()) => {
                *self = next;
                Ok(Retirement::Stored)
            }
            Err(Error::Storage(error)) if error.code == "STORAGE_PRESSURE" => {
                Ok(Retirement::Deferred(error.message))
            }
            Err(error) => Err(error),
        }
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

pub(crate) fn encode_clock(
    mut out: crate::sparrow_capnp::optional_instant::Builder<'_>,
    clock: Option<u64>,
) {
    match clock {
        Some(at) => out.init_value().set_unix_millis(at),
        None => out.set_absent(()),
    }
}

/// One claim listing on the wire: the window it answers and every listed claim.
pub(crate) fn encode_claims(
    mut out: crate::sparrow_capnp::watch_claims::Builder<'_>,
    store: &str,
    window: (u64, u64, u64),
    claims: &[sparrow_core::ClaimListing],
) {
    use crate::sparrow_capnp::ClaimState as Wire;
    let (as_of, since, until) = window;
    out.set_store_id(store);
    out.reborrow().init_as_of().set_unix_millis(as_of);
    out.reborrow().init_since().set_unix_millis(since);
    out.reborrow().init_until().set_unix_millis(until);
    let mut rows = out.init_claims(claims.len() as u32);
    for (index, claim) in claims.iter().enumerate() {
        let mut row = rows.reborrow().get(index as u32);
        let journal = &claim.journal;
        let history = &journal.history;
        row.set_claim_id(&journal.import_id);
        row.set_watch(&journal.watch);
        row.set_state(match claim.state {
            sparrow_core::ClaimState::Pending => Wire::Pending,
            sparrow_core::ClaimState::Complete => Wire::Complete,
            sparrow_core::ClaimState::Abandoned => Wire::Abandoned,
        });
        row.set_relative_path(&journal.relative_path);
        encode_clock(row.reborrow().init_first_seen(), history.first_seen);
        encode_clock(row.reborrow().init_last_attempt(), history.last_attempt_at);
        row.set_attempts(history.attempts);
        encode_clock(
            row.reborrow().init_last_pending_at(),
            history.last_pending_at,
        );
        match &history.last_pending_reason {
            Some(reason) => row.reborrow().init_last_pending_reason().set_value(reason),
            None => row.reborrow().init_last_pending_reason().set_absent(()),
        }
        encode_clock(row.init_finished_at(), claim.finished_at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_core::Config;

    /// The stored watch shape an older build reads: the same fields, without `publish_hold`.
    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct OldConsumeWatch {
        id: String,
        root: std::path::PathBuf,
        producer_contract: crate::ProducerContract,
        source: String,
        format: String,
        quiet_ms: u32,
        max_directories: usize,
        max_candidates: usize,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct OldManagedWatch {
        definition: OldConsumeWatch,
        revision: u64,
        enabled: bool,
    }

    fn managed(publish_hold: bool) -> ManagedWatch {
        ManagedWatch {
            definition: ConsumeWatch {
                id: "stored".into(),
                root: "/srv/producer".into(),
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: "stored".into(),
                format: "jsonl".into(),
                quiet_ms: 1000,
                max_directories: 16,
                max_candidates: 64,
                publish_hold,
            },
            revision: 3,
            enabled: true,
        }
    }

    #[test]
    fn an_unheld_watch_is_stored_as_older_builds_read_it_and_a_held_one_is_refused_by_them() {
        // INVARIANT (issue 280, publish hold): with the hold off the stored catalog is the old
        // bytes; with it on, an older build refuses the catalog by naming the field.
        let unheld = toml::to_string(&managed(false)).unwrap();
        assert!(!unheld.contains("publish_hold"));
        let old = OldManagedWatch {
            definition: OldConsumeWatch {
                id: "stored".into(),
                root: "/srv/producer".into(),
                producer_contract: crate::ProducerContract::FinishedFiles,
                source: "stored".into(),
                format: "jsonl".into(),
                quiet_ms: 1000,
                max_directories: 16,
                max_candidates: 64,
            },
            revision: 3,
            enabled: true,
        };
        assert_eq!(toml::to_string(&old).unwrap(), unheld);
        assert!(
            !toml::from_str::<ManagedWatch>(&unheld)
                .unwrap()
                .definition
                .publish_hold
        );

        let held = toml::to_string(&managed(true)).unwrap();
        assert!(held.contains("publish_hold = true"));
        let refused = toml::from_str::<OldManagedWatch>(&held)
            .err()
            .expect("an older shape refuses a held catalog")
            .to_string();
        assert!(refused.contains("publish_hold"), "{refused}");
    }

    /// INVARIANT (#440): a retirement that the real store refuses under pressure leaves the
    /// stored registry and the catalog as they were and the work due, so the next pass stores
    /// it; the refusal does not end the worker or lose the work.
    #[test]
    fn a_retirement_refused_by_the_store_stays_due_and_is_stored_by_the_next_pass() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
        let mut disabled = managed(false);
        disabled.enabled = false;
        let mut catalog = Catalog {
            state: State {
                version: 1,
                store_id: "store:test".into(),
                revision: 3,
                watches: vec![disabled],
                receipts: Vec::new(),
            },
            raw: store.watch_registry().unwrap(),
            configured: std::collections::BTreeMap::new(),
        };
        let before = store.watch_registry().unwrap();
        store.space_floor().refuse_writes_for_tests(true);
        let refused = catalog.retire_and_store(&[], |next| next.persist(&store));
        assert!(matches!(refused, Ok(Retirement::Deferred(_))));
        assert_eq!(
            catalog.watches().len(),
            1,
            "a refused write changed the catalog"
        );
        assert_eq!(store.watch_registry().unwrap(), before);
        store.space_floor().refuse_writes_for_tests(false);
        let stored = catalog.retire_and_store(&[], |next| next.persist(&store));
        assert!(matches!(stored, Ok(Retirement::Stored)));
        assert!(catalog.watches().is_empty());
        assert_ne!(store.watch_registry().unwrap(), before);
        let none = catalog.retire_and_store(&[], |next| next.persist(&store));
        assert!(matches!(none, Ok(Retirement::Nothing)));
    }
}
