// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Borrowed native explanation entries and transport capacity; no repricing.

use super::Error;
use purrdf::sparql::{NodeCharges, PfDescriptor, QueryExplanation, shapes::ShapeReport};

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
    /// Native ordinary property-function declaration, including its actual modes.
    Relation(&'a PfDescriptor),
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
            .chain(
                value
                    .relations()
                    .iter()
                    .map(SparqlExplanationItem::Relation),
            )
    })
}

pub(super) fn metadata_bytes(value: &QueryExplanation) -> Result<usize, Error> {
    // This input registers ordinary stored-text relations. Refuse a changed
    // registry contract rather than omitting a declaration the wire cannot carry.
    if !value.aggregates().is_empty()
        || value
            .relations()
            .iter()
            .any(|relation| relation.ranked.is_some())
    {
        return Err(Error::new(
            "UNSUPPORTED",
            "this explanation transport requires ordinary property functions and no custom aggregates",
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
    for relation in value.relations() {
        bytes = bytes
            .saturating_add(256)
            .saturating_add(relation.iri.len().saturating_mul(6));
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
        SparqlExplanationItem::Relation(relation) => relation.modes.iter().fold(
            4096usize.saturating_add(relation.iri.len().saturating_mul(6)),
            |bytes, mode| {
                bytes
                    .saturating_add(256)
                    .saturating_add(mode.code.len().saturating_mul(6))
            },
        ),
    };
    Ok(bytes)
}
