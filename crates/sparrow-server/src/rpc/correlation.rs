// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed fixed-recipe requests through the original native result owner.

use super::*;
use sparrow_core::CorrelateRead;

pub(super) async fn query(
    rpc: &Rpc,
    params: wire::sparrow::CorrelateParams,
    results: &mut wire::sparrow::CorrelateResults,
) -> capnp::Result<()> {
    let input = params.get()?.get_query()?;
    let decode = || -> Result<_, Error> {
        let scope = sparql::query_scope(
            rpc,
            input.get_schema_version(),
            input.get_store_id().map_err(invalid)?,
            input.get_scope().map_err(invalid)?,
        )?;
        let selected = input.get_selector().map_err(invalid)?;
        if let wire::correlation_selector::Evidence(ids) = selected.which().map_err(invalid)? {
            let ids = ids.map_err(invalid)?;
            if ids.len() > 32 {
                return Err(invalid("correlation admits at most 32 seed references"));
            }
            for reference in ids {
                let owner = reference.get_store_id().map_err(invalid)?;
                if owner.len() > 128 {
                    return Err(invalid("seed store ID exceeds 128 bytes"));
                }
                rpc.check_store(owner.to_str().map_err(invalid)?)?;
            }
        }
        let selector =
            crate::correlation_wire::selector(selected, &rpc.store.store_id()?).map_err(invalid)?;
        let relation = crate::correlation_wire::relation(input.get_relation().map_err(invalid)?)
            .map_err(invalid)?;
        let (source, governors) = sparql::query_limits(input.get_limits().map_err(invalid)?)?;
        Ok((scope, selector, relation, source, governors))
    };
    let (scope, selector, relation, source, governors) = match decode() {
        Ok(value) => value,
        Err(error) => {
            failure(results.get().init_result(), error);
            return Ok(());
        }
    };
    // Keep the same worker, cancellation, captured-result and delivery owner.
    let reply = rpc
        .query_work(true, move |store, now, cancellation| {
            store
                .correlate_read(
                    &CorrelateRead {
                        scope: &scope,
                        selector: &selector,
                        relation,
                        governors: &governors,
                        limits: source,
                    },
                    now,
                    &cancellation,
                )
                .map_err(|error| Error::new(error.code, error.message.clone()))
        })
        .await;
    sparql::deliver(results.get().init_result(), reply);
    Ok(())
}
