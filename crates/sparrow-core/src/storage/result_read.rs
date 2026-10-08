// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Cursor-only continuation of already captured results; no membership re-evaluation.

use super::{Error, Store, catalog_lease, record_read};
use crate::{ArtifactReadPage, PinReadPage, RecordReadPage, SourceReadPage};

/// Operation identity and the original typed captured page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultPage {
    /// Native combined record qualification, with optional ranked text.
    Records(RecordReadPage),
    /// Standalone native text ranking.
    Text(RecordReadPage),
    /// Native key/kind counts.
    Keys(RecordReadPage),
    /// Ordered native JSON value occurrences.
    Values(RecordReadPage),
    /// Artifact summaries and scope coverage.
    Artifacts(ArtifactReadPage),
    /// Exact intake source inventory.
    Sources(SourceReadPage),
    /// Active collection summaries or captured direct members.
    Pins(PinReadPage),
}

impl Store {
    /// Resume the cursor's captured operation, selector and limits without renewing its lease.
    pub fn result_next(&self, store_id: &str, cursor: &str, now: u64) -> Result<ResultPage, Error> {
        if self.lock()?.id != store_id {
            return Err(Error::new(
                "STORE_MISMATCH",
                "result cursor requires its original store identity",
            ));
        }
        let cursor = catalog_lease::Cursor::parse(cursor)?;
        {
            let mut reads = self
                .record_reads
                .lock()
                .map_err(|_| Error::new("STORAGE_FAILURE", "query read registry poisoned"))?;
            reads.purge(now);
            if let Some((selector, page)) = reads.resume_captured(&cursor, now)? {
                return Ok(match selector {
                    record_read::Selector::Records(query) => match query.operation {
                        crate::RecordOperation::Records => ResultPage::Records(page),
                        crate::RecordOperation::Text => ResultPage::Text(page),
                    },
                    record_read::Selector::Json(query) => match query.query {
                        crate::JsonQuery::Keys => ResultPage::Keys(page),
                        crate::JsonQuery::Values { .. } => ResultPage::Values(page),
                    },
                });
            }
        }
        {
            let mut reads = self
                .artifact_reads
                .lock()
                .map_err(|_| Error::new("STORAGE_FAILURE", "artifact read registry poisoned"))?;
            reads.purge(now);
            if let Some((_, page)) = reads.resume_captured(&cursor, now)? {
                return Ok(ResultPage::Artifacts(page));
            }
        }
        {
            let mut reads = self
                .source_reads
                .lock()
                .map_err(|_| Error::new("STORAGE_FAILURE", "source read registry poisoned"))?;
            reads.purge(now);
            if let Some((_, page)) = reads.resume_captured(&cursor, now)? {
                return Ok(ResultPage::Sources(page));
            }
        }
        {
            let mut reads = self
                .pin_reads
                .lock()
                .map_err(|_| Error::new("STORAGE_FAILURE", "pin read registry poisoned"))?;
            reads.purge(now);
            if let Some((_, page)) = reads.resume_captured(&cursor, now)? {
                return Ok(ResultPage::Pins(page));
            }
        }
        Err(Error::new(
            "RESULT_EXPIRED",
            "result handle is expired, unknown, or from a previous store owner",
        ))
    }
}
