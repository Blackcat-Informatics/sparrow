// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Persistent evidence ownership and typed Sparrow operations.

/// Namespace shared by occurrence RDF generation and agent vocabulary discovery.
pub const RDF_VOCABULARY_NAMESPACE: &str = "urn:sparrow:vocab:";

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
pub use storage::archive::{
    ARCHIVE_MAX_SEGMENT_BYTES, ARCHIVE_MIN_SEGMENT_BYTES, ARCHIVE_SEGMENT_BYTES,
    ARCHIVE_SEGMENT_RECORDS, ARCHIVE_START_FRACTION, ArchiveIntent, ArchivePolicy, ArchiveState,
};
pub use storage::artifact_read::{
    ArtifactOrigin, ArtifactPageBody, ArtifactRead, ArtifactReadPage, ArtifactSelector,
    ArtifactSummary,
};
pub use storage::backup::{
    BackupPhase, BackupStatus, BackupTarget, NETWORK_FILESYSTEMS, restore as restore_backup,
};
pub use storage::catalog_lease::CatalogReadPage;
pub use storage::consumption::{
    ClaimHistory, ClaimListing, ClaimState, ConsumptionDirectories, ConsumptionGeneration,
    ConsumptionJournal, ConsumptionRetirement, MAX_CONSUMPTION_JOURNALS, MAX_LISTED_CLAIMS,
    MAX_RETIRED_CONSUMPTION_CLAIMS, RetiredConsumption, validate_consumption_path,
};
pub use storage::context::{
    CONTEXT_CAPTURE_PROFILE, Context, ContextFrontier, ContextLimits, ContextSide,
};
pub use storage::correlation::{
    CORRELATION_PROFILE, CorrelateRead, CorrelationCapture, CorrelationClock, CorrelationInterval,
    CorrelationRelation, CorrelationSelector,
};
pub use storage::diagnostic::{
    DiagnosticCoverage, DiagnosticDimension, DiagnosticProfile, DiagnosticSnapshot, DiagnosticValue,
};
pub use storage::durability::{
    DEFAULT_FLUSH_INTERVAL_MS, Durability, DurabilityStatus, DurabilityWatermark,
    MAX_BUFFERED_BYTES, MAX_BUFFERED_WRITES, MAX_FLUSH_INTERVAL_MS, MIN_FLUSH_INTERVAL_MS,
};
#[cfg(feature = "test-support")]
pub use storage::eviction::RetirementBounds;
pub use storage::eviction::{
    EvictionFrontier, EvictionLimits, EvictionReport, MAX_PINNED_MEMBERS, MAX_PINS,
};
pub use storage::expiry::{EXPIRY_CEILING_MS, ExpiryFrontier, ExpiryReport};
pub use storage::forced_compaction::FamilySize;
#[cfg(feature = "test-support")]
pub use storage::forced_compaction::Fault;
pub use storage::handoff::ConsumptionHandoff;
pub use storage::import::{
    IMPORT_CHUNK_BYTES, IMPORT_LAYOUT_PAGE_RECORDS, ImportLayoutPage, ImportManifest,
    ImportRecordSpan, ImportStart, MAX_IMPORT_BYTES, RAW_RECORD_BYTES,
};
pub use storage::ingest_status::{
    IngestDeletion, IngestIndexCounts, IngestReadLimits, IngestSelector, IngestStatus,
};
pub use storage::intake::{INGEST_SOCKET, Intake, PeerCredential, admit_import};
pub use storage::listing_build::BuildProgress;
pub use storage::log_time::LogTimeCapture;
pub use storage::migration::{Migration, MigrationPage, MigrationStatus};
pub use storage::open_files::OpenFiles;
pub use storage::pin_read::PAGE_BYTES as PIN_READ_PAGE_BYTES;
pub use storage::pin_read::{PinPageBody, PinRead, PinReadPage, PinSelector, PinSummary};
pub use storage::pressure::{PressurePhase, PressureStatus};
pub use storage::producer_stream::{
    MAX_PRODUCER_GAPS, MAX_PRODUCER_REGISTRY_BYTES, MAX_PRODUCER_STREAMS, ProducerBatch,
    ProducerGap, ProducerPosition, ProducerStream,
};
pub use storage::publication::{ImportPreparation, RawPreparation};
pub use storage::rdf_view::{
    NativePageBody, RDF_SOURCE_ADMISSION_PROFILE, RdfGraphLayers, RdfReadEvidence, RdfReadLimits,
    RdfScopeCoverage, ReadError as RdfReadError, STORED_TEXT_CORPUS_PROFILE,
    STORED_TEXT_RELATION_PROFILE, ScopedSparqlExplanation, ScopedSparqlResult,
    SparqlExplanationItem, SparqlExplanationPageBody, SparqlExplanationReadPage, SparqlGraphKind,
    SparqlItem, SparqlPageBody, SparqlRead, SparqlReadPage, SparqlTextCapture, SparqlTextCoverage,
    SparqlTextRelations, StoredRdfView, sparql_term_node_bytes, sparql_term_wire_bytes,
};
pub use storage::receiver_stream::{
    MAX_RECEIVER_REGISTRY_BYTES, MAX_RECEIVER_WINDOWS, ReceiverDiscard, ReceiverPosition,
    ReceiverWindow, SyslogReceiver,
};
pub use storage::recompression::{
    RECOMPRESSION_SETTLE_SECONDS, RecompressionPhase, RecompressionStatus,
};
pub use storage::record_read::{
    JsonRead, QUERY_CAPTURE_PROFILE, QueryCancellation, RecordOperation, RecordPageBody,
    RecordRead, RecordReadPage,
};
pub use storage::result_read::ResultPage;
pub use storage::retention_metrics::{RetentionMetrics, RetentionReadLimits};
#[cfg(feature = "test-support")]
pub use storage::retire_artifact_rdf_for_legacy_fixture;
pub use storage::source_read::{
    SourcePageBody, SourceRead, SourceReadPage, SourceSelector, SourceSummary, SourceSyslogBinding,
    SourceWatchBinding, SourceWatchInfo,
};
pub use storage::source_time::{
    EventPlacement, EventTime, EventTimeBasis, SourceTimeProfile, TimeRepresentation,
};
pub use storage::space::{FloorStatus, SpaceFloor};
#[cfg(feature = "test-support")]
pub use storage::store_event_fallback_rdf_for_legacy_fixture;
pub use storage::stream_evidence::{
    LostStream, LostStreamKind, MAX_NAMED_LOST_STREAMS, StreamEvidenceLoss,
};
pub use storage::syslog::{
    SYSLOG_ELEMENTS, SYSLOG_MESSAGE_BYTES, SYSLOG_PARAMETERS, SyslogDialect, SyslogElement,
    SyslogField, SyslogParameter, SyslogSnapshot, syslog_profile_id,
};
pub use storage::tail::{
    MAX_TAIL_GENERATIONS, MAX_TAIL_REGISTRY_BYTES, TailFinish, TailFinishIntent, TailGeneration,
    TailPosition, TailPublication, TailStart,
};
pub use storage::watch_registry::MAX_WATCH_REGISTRY_BYTES;
pub use storage::{
    Artifact, Config, Error, INLINE_ARTIFACT_BYTES, OpenPhase, Pin, QUERY_SCOPE_PROFILE,
    QUERY_SCOPE_WORKSPACE_BYTES, RETENTION_MS, Record, RetentionWaker, Store,
};
#[cfg(feature = "test-support")]
pub use storage::{
    SourceCountAudit, StoreCounterAudit, remove_source_summary_for_legacy_fixture,
    remove_store_counters_for_legacy_fixture,
};
pub use text::resolve_text_analyzer;
