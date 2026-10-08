// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One typed adapter from immutable client intent to durable consumption ownership.

use crate::sparrow_capnp as wire;
use sparrow_client::CompletedHandoff;
use sparrow_core::{ConsumptionHandoff, ConsumptionJournal, Error};

pub(crate) fn intent(value: &CompletedHandoff) -> Result<ConsumptionHandoff, Error> {
    Ok(ConsumptionHandoff {
        request_id: value.request_id.clone(),
        watch: value.watch.clone(),
        watch_revision: value.watch_revision,
        relative_path: value.relative_path.clone(),
        tail_generation_id: value.tail_generation_id.clone(),
        observed: crate::consumption::generation(value.generation)
            .map_err(|e| Error::new("INVALID_ARGUMENT", e.to_string()))?,
    })
}

pub(crate) fn result(
    mut out: wire::result::Builder<'_, wire::handoff_receipt::Owned>,
    request: &CompletedHandoff,
    result: Result<ConsumptionJournal, Error>,
) -> capnp::Result<()> {
    match result {
        Ok(journal) => {
            let mut receipt = out.reborrow().init_ok();
            request
                .encode(receipt.reborrow().init_request())
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            receipt.set_import_id(&journal.import_id);
            receipt.set_phase(if journal.removed_at.is_some() {
                wire::HandoffPhase::Removed
            } else if journal.published_id().is_some() {
                wire::HandoffPhase::Published
            } else if journal.claimed.is_some() {
                wire::HandoffPhase::Claimed
            } else {
                wire::HandoffPhase::Reserved
            });
            let mut artifact = receipt.reborrow().init_artifact_id();
            match &journal.published_artifact {
                Some(id) => artifact.set_value(id),
                None => artifact.set_absent(()),
            }
            sparrow_client::encode_optional(
                receipt.reborrow().init_tail_generation_id(),
                journal
                    .tail
                    .as_ref()
                    .filter(|_| journal.tail_published)
                    .map(|tail| tail.id.as_str()),
                |mut out, value| out.set_value(value),
                |mut out| out.set_absent(()),
            );
            receipt.set_durability(wire::Durability::Durable);
        }
        Err(error) => crate::rpc::failure(out, error),
    }
    Ok(())
}
