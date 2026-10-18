// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Merge physical graph-prefixed cursors without scanning unrelated statement rows.
//! Keys use the existing persisted SPOG order; native quad IDs retain identity.

use super::*;
use std::{cmp::Reverse, collections::BinaryHeap, iter::Peekable};

pub(super) enum RowCursor<'a> {
    Indexed(QuadIter<'a>),
    Scoped(ScopedRows<'a>),
    // DatasetView iterators report failures through their sticky read fence.
    // Retain the original cause; this never certifies a successful empty stream.
    Refused { _cause: ReadError },
}

impl<'a> RowCursor<'a> {
    pub(super) fn scoped(view: &'a StoredRdfView<'a>, stream: u8, union_default: bool) -> Self {
        match ScopedRows::new(view, stream, union_default) {
            Ok(rows) => Self::Scoped(rows),
            Err(error) => Self::Refused { _cause: error },
        }
    }
}

impl Iterator for RowCursor<'_> {
    type Item = QuadIds<GlobalTermId>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Indexed(rows) => rows.next(),
            Self::Scoped(rows) => rows.next(),
            Self::Refused { .. } => None,
        }
    }
}

pub(super) struct ScopedRows<'a> {
    view: &'a StoredRdfView<'a>,
    cursors: Vec<Peekable<QuadIter<'a>>>,
    heads: BinaryHeap<Reverse<([u64; 4], usize)>>,
    previous: Option<QuadIds<GlobalTermId>>,
    initial: Option<(u8, bool)>,
    admission: Reservation,
}

impl<'a> ScopedRows<'a> {
    fn new(
        view: &'a StoredRdfView<'a>,
        stream: u8,
        union_default: bool,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            view,
            cursors: Vec::new(),
            heads: BinaryHeap::new(),
            previous: None,
            initial: Some((stream, union_default)),
            admission: view.budget.reserve(512)?,
        })
    }

    fn initialize(&mut self) -> Result<(), ReadError> {
        let Some((stream, union_default)) = self.initial.take() else {
            return Ok(());
        };
        let view = self.view;
        if view.owners.is_empty() {
            return Ok(());
        }
        for graph in view.graph_rows() {
            // GSPO is the existing graph-leading index. Its tail is SPO order,
            // so a heap can retain the original global order in O(graphs) space.
            let mut cursor = view
                .physical_rows(
                    stream,
                    3,
                    [None; 3],
                    GraphMatch::Named(graph),
                    union_default,
                )
                .peekable();
            if cursor.peek().is_none() {
                continue;
            }
            self.grow()?;
            let index = self.cursors.len();
            self.cursors.push(cursor);
            self.push_head(index);
        }
        if let Some(error) = view.budget.checkpoint().0 {
            return Err(error);
        }
        Ok(())
    }

    fn grow(&mut self) -> Result<(), ReadError> {
        if self.cursors.len() < self.cursors.capacity().min(self.heads.capacity()) {
            return Ok(());
        }
        let capacity = self
            .cursors
            .len()
            .checked_add(1)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(|| {
                self.view
                    .budget
                    .fail(Error::new("RESOURCE_LIMIT", "RDF cursor count overflow"))
            })?;
        let bytes = size_of::<Peekable<QuadIter<'a>>>()
            .checked_add(size_of::<Reverse<([u64; 4], usize)>>())
            .and_then(|size| size.checked_mul(capacity))
            .and_then(|n| n.checked_add(512))
            .ok_or_else(|| {
                self.view.budget.fail(Error::new(
                    "RESOURCE_LIMIT",
                    "RDF cursor queue capacity overflow",
                ))
            })?;
        self.admission.resize(bytes as u64)?;
        self.cursors
            .try_reserve_exact(capacity - self.cursors.len())
            .map_err(|_| {
                self.view.budget.fail(Error::new(
                    "RESOURCE_LIMIT",
                    "RDF cursor allocation refused",
                ))
            })?;
        self.heads
            .try_reserve_exact(capacity - self.heads.len())
            .map_err(|_| {
                self.view.budget.fail(Error::new(
                    "RESOURCE_LIMIT",
                    "RDF cursor head allocation refused",
                ))
            })?;
        Ok(())
    }

    fn push_head(&mut self, index: usize) {
        if let Some(row) = self.cursors[index].peek() {
            let key = [
                ordinal(row.s),
                ordinal(row.p),
                ordinal(row.o),
                row.g.map_or(0, ordinal),
            ];
            self.heads.push(Reverse((key, index)));
        }
    }
}

impl Iterator for ScopedRows<'_> {
    type Item = QuadIds<GlobalTermId>;
    fn next(&mut self) -> Option<Self::Item> {
        // An unused native iterator must not scan declarations or open all
        // graph heads. Initialization failures remain in the read fence.
        if self.initialize().is_err() {
            return None;
        }
        loop {
            if self.view.budget.checkpoint().0.is_some() {
                return None;
            }
            let Reverse((_, index)) = self.heads.pop()?;
            let Some(row) = self.cursors[index].next() else {
                self.view.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "RDF cursor head lost its admitted row",
                ));
                return None;
            };
            self.push_head(index);
            if self.view.budget.checkpoint().0.is_some() {
                return None;
            }
            if self.previous == Some(row) {
                continue;
            }
            self.previous = Some(row);
            return Some(row);
        }
    }
}
