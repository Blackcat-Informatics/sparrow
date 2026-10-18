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
    EventInterval, FieldConstraint, JSON_QUERY_PATH_BYTES, JsonPredicate, JsonQuery,
    QueryAlternatives, QueryScope, QuerySelection, RecordQuery,
};
pub use storage::artifact_read::{
    ArtifactOrigin, ArtifactPageBody, ArtifactRead, ArtifactReadPage, ArtifactSelector,
    ArtifactSummary,
};
pub use storage::catalog_lease::CatalogReadPage;
pub use storage::consumption::{
    ConsumptionDirectories, ConsumptionGeneration, ConsumptionJournal, MAX_CONSUMPTION_JOURNALS,
    validate_consumption_path,
};
pub use storage::context::{
    CONTEXT_CAPTURE_PROFILE, Context, ContextFrontier, ContextLimits, ContextSide,
};
pub use storage::diagnostic::{
    DiagnosticCoverage, DiagnosticDimension, DiagnosticProfile, DiagnosticSnapshot, DiagnosticValue,
};
pub use storage::durability::{
    DEFAULT_FLUSH_INTERVAL_MS, Durability, DurabilityStatus, DurabilityWatermark,
    MAX_BUFFERED_BYTES, MAX_BUFFERED_WRITES,
};
pub use storage::eviction::{EvictionFrontier, EvictionLimits, EvictionReport};
pub use storage::expiry::{ExpiryFrontier, ExpiryReport};
pub use storage::handoff::ConsumptionHandoff;
pub use storage::import::{
    IMPORT_CHUNK_BYTES, IMPORT_LAYOUT_PAGE_RECORDS, ImportLayoutPage, ImportManifest,
    ImportRecordSpan, ImportStart,
};
pub use storage::ingest_status::{
    IngestDeletion, IngestIndexCounts, IngestReadLimits, IngestSelector, IngestStatus,
};
pub use storage::pin_read::PAGE_BYTES as PIN_READ_PAGE_BYTES;
pub use storage::pin_read::{PinPageBody, PinRead, PinReadPage, PinSelector, PinSummary};
pub use storage::pressure::{PressurePhase, PressureStatus};
pub use storage::producer_stream::{
    MAX_PRODUCER_GAPS, MAX_PRODUCER_REGISTRY_BYTES, MAX_PRODUCER_STREAMS, ProducerBatch,
    ProducerGap, ProducerPosition, ProducerStream,
};
pub use storage::publication::{ImportPreparation, RawPreparation};
pub use storage::rdf_view::{
    RdfGraphLayers, RdfReadEvidence, RdfReadLimits, RdfScopeCoverage, ReadError as RdfReadError,
    ScopedSparqlResult, SparqlGraphKind, SparqlItem, SparqlPageBody, SparqlRead, SparqlReadPage,
    StoredRdfView,
};
pub use storage::receiver_stream::{
    MAX_RECEIVER_REGISTRY_BYTES, MAX_RECEIVER_WINDOWS, ReceiverDiscard, ReceiverPosition,
    ReceiverWindow, SyslogReceiver,
};
pub use storage::record_read::{
    JsonRead, QUERY_CAPTURE_PROFILE, QueryCancellation, RecordOperation, RecordPageBody,
    RecordRead, RecordReadPage,
};
pub use storage::result_read::ResultPage;
pub use storage::retention_metrics::{RetentionMetrics, RetentionReadLimits};
pub use storage::source_read::{
    SourcePageBody, SourceRead, SourceReadPage, SourceSelector, SourceSummary, SourceSyslogBinding,
    SourceWatchBinding, SourceWatchInfo,
};
pub use storage::source_time::{EventTime, SourceTimeProfile, TimeRepresentation};
pub use storage::syslog::{
    SYSLOG_ELEMENTS, SYSLOG_MESSAGE_BYTES, SYSLOG_PARAMETERS, SyslogDialect, SyslogElement,
    SyslogField, SyslogParameter, SyslogSnapshot, syslog_profile_id,
};
pub use storage::tail::{
    MAX_TAIL_GENERATIONS, MAX_TAIL_REGISTRY_BYTES, TailFinish, TailFinishIntent, TailGeneration,
    TailPosition, TailPublication, TailStart,
};
pub use storage::watch_registry::MAX_WATCH_REGISTRY_BYTES;
pub use storage::{Artifact, Config, Error, Pin, Record, Store};
pub use text::resolve_text_analyzer;
