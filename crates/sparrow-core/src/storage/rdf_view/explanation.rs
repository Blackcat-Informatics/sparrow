// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Borrowed native explanation entries and transport capacity; no repricing.

use super::Error;
use purrdf::sparql::{NodeCharges, QueryExplanation, shapes::ShapeReport};

/// One structured native measured-work entry, retaining its original native owner.
#[derive(Debug)]
pub enum SparqlExplanationItem<'a> {
    /// Native logical node with charge-point counters and independent estimates.
    Node(&'a NodeCharges),
    /// Native planner's selected BGP order entry.
    JoinOrder(&'a str),
    /// Native statistical input, including provider/revision/exactness when known.
    Statistic(&'a str),
    /// Native selected physical strategy and actual measured work.
    Shape(&'a ShapeReport),
}

pub(super) fn items(
    value: Option<&QueryExplanation>,
) -> impl Iterator<Item = SparqlExplanationItem<'_>> {
    value.into_iter().flat_map(|value| {
        value
            .ledger()
            .iter()
            .map(SparqlExplanationItem::Node)
            .chain(
                value
                    .join_orders()
                    .iter()
                    .map(|text| SparqlExplanationItem::JoinOrder(text)),
            )
            .chain(
                value
                    .statistics()
                    .iter()
                    .map(|text| SparqlExplanationItem::Statistic(text)),
            )
            .chain(value.shapes().iter().map(SparqlExplanationItem::Shape))
    })
}

pub(super) fn metadata_bytes(value: &QueryExplanation) -> Result<usize, Error> {
    // This endpoint supplies QueryOptions::default(), so no host registry is in
    // scope. Refuse an integration invariant change instead of dropping declarations.
    if !value.relations().is_empty() || !value.aggregates().is_empty() {
        return Err(Error::new(
            "UNSUPPORTED",
            "explanation transport requires descriptors for the injected native extension registry",
        ));
    }
    let profile = value.profile();
    let mut bytes = 4096usize.saturating_add(
        profile
            .id
            .len()
            .saturating_add(profile.digest.len())
            .saturating_mul(6),
    );
    for (label, _) in purrdf::sparql::CHARGE_SCHEDULE {
        bytes = bytes
            .saturating_add(label.len().saturating_mul(6))
            .saturating_add(256);
    }
    Ok(bytes)
}

pub(super) fn item_bytes(value: SparqlExplanationItem<'_>) -> Result<usize, Error> {
    let bytes = match value {
        SparqlExplanationItem::Node(node) => 4096usize
            .saturating_add(node.label.len().saturating_mul(6))
            .saturating_add(purrdf::sparql::CHARGE_SCHEDULE.len().saturating_mul(128)),
        SparqlExplanationItem::JoinOrder(text) | SparqlExplanationItem::Statistic(text) => {
            1024usize.saturating_add(text.len().saturating_mul(6))
        }
        SparqlExplanationItem::Shape(shape) => 4096usize.saturating_add(
            shape
                .operator
                .map_or(0, str::len)
                .saturating_add(shape.provider.map_or(0, str::len))
                .saturating_mul(6),
        ),
    };
    Ok(bytes)
}
