// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Persistent evidence ownership and typed Sparrow operations.

mod instant;
mod projection;
mod query;
mod storage;
mod text;

pub use instant::{format_instant, parse_instant};
pub use projection::json_profile as json_projection_profile;
pub use query::{
    EventInterval, FieldConstraint, JsonPredicate, JsonQuery, QueryScope, QuerySelection,
    RecordQuery,
};
pub use storage::consumption::{
    ConsumptionDirectories, ConsumptionGeneration, ConsumptionJournal, MAX_CONSUMPTION_JOURNALS,
};
pub use storage::context::{Context, ContextFrontier, ContextSide};
pub use storage::durability::{
    DEFAULT_FLUSH_INTERVAL_MS, Durability, DurabilityStatus, DurabilityWatermark,
    MAX_BUFFERED_BYTES, MAX_BUFFERED_WRITES,
};
pub use storage::import::{
    IMPORT_CHUNK_BYTES, IMPORT_LAYOUT_PAGE_RECORDS, ImportLayoutPage, ImportManifest,
    ImportRecordSpan, ImportStart,
};
pub use storage::pin_read::PAGE_BYTES as PIN_READ_PAGE_BYTES;
pub use storage::pin_read::{PinPageBody, PinRead, PinReadPage, PinSelector, PinSummary};
pub use storage::publication::{ImportPreparation, RawPreparation};
pub use storage::rdf_view::{
    RdfReadEvidence, RdfReadLimits, ReadError as RdfReadError, StoredRdfView,
};
pub use storage::source_time::{EventTime, SourceTimeProfile, TimeRepresentation};
pub use storage::watch_registry::MAX_WATCH_REGISTRY_BYTES;
pub use storage::{Artifact, Config, Error, Pin, Record, Store};
pub use text::resolve_text_analyzer;
