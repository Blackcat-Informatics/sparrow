@0xdcc9fefce9188f9a;
# SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
# SPDX-License-Identifier: AGPL-3.0-only

struct Instant { unixMillis @0 :UInt64; }
struct Failure { code @0 :Text; message @1 :Text; retryable @2 :Bool; }
struct Result(T) { union { ok @0 :T; error @1 :Failure; } }
struct OptionalText { union { absent @0 :Void; value @1 :Text; } }
struct Evidence { storeId @0 :Text; union { artifact @1 :Text; record @2 :Text; } }
enum Format { jsonl @0; log @1; json @2; markdown @3; opaque @4; }
enum Durability { buffered @0; durable @1; }
struct DurabilityWatermark { storeId @0 :Text; sessionId @1 :Text; sequence @2 :UInt64; }
struct OptionalDurabilityWatermark { union { absent @0 :Void; value @1 :DurabilityWatermark; } }
struct DurabilityStatus {
  sessionId @0 :Text; acceptedThrough @1 :UInt64; durableThrough @2 :UInt64;
  pendingWrites @3 :UInt32; pendingBytes @4 :UInt64;
  oldestPendingMs @5 :UInt64; failure @6 :OptionalText;
  maxPendingWrites @7 :UInt32; maxPendingBytes @8 :UInt64;
  flushIntervalMs @9 :UInt32; flushLagMs @10 :UInt64;
}
enum JsonKind { string @0; number @1; booleanTrue @2; booleanFalse @3; null @4; array @5; object @6; }
struct OptionalKind { union { absent @0 :Void; value @1 :JsonKind; } }
struct Artifact {
  storeId @0 :Text; id @1 :Text; source @2 :Text; format @3 :Format;
  receivedAt @4 :Instant; expiresAt @5 :Instant; byteLength @6 :UInt64;
  payloadId @7 :Text; recordIds @8 :List(Text);
  durability @9 :Durability = durable;
  bufferedThrough @10 :OptionalDurabilityWatermark;
}
struct ArtifactRef { storeId @0 :Text; id @1 :Text; }
struct RecordRef { storeId @0 :Text; id @1 :Text; }
struct Record {
  storeId @0 :Text; id @1 :Text; artifactId @2 :Text; source @3 :Text;
  receivedAt @4 :Instant; expiresAt @5 :Instant;
  byteStart @6 :UInt64; byteEnd @7 :UInt64;
  eventTime @8 :EventTime;
  diagnostics @9 :DiagnosticSnapshot;
}
enum DiagnosticDimension { service @0; host @1; severity @2; traceId @3; spanId @4; }
struct DiagnosticMapping { dimension @0 :DiagnosticDimension; path @1 :Text; }
struct DiagnosticDefinition { source @0 :Text; mappings @1 :List(DiagnosticMapping); }
struct DiagnosticProfile { storeId @0 :Text; id @1 :Text; source @2 :Text; mappings @3 :List(DiagnosticMapping); }
struct OptionalDiagnosticProfile { union { absent @0 :Void; value @1 :DiagnosticProfile; } }
struct KnownDiagnostic { value @0 :Text; occurrence @1 :UInt64; sourceStart @2 :UInt64; sourceEnd @3 :UInt64; }
struct DiagnosticField { dimension @0 :DiagnosticDimension; union { unknown @1 :Text; known @2 :KnownDiagnostic; } }
struct DiagnosticSnapshot { profileId @0 :OptionalText; fields @1 :List(DiagnosticField); }
struct DiagnosticFilter { dimension @0 :DiagnosticDimension; alternatives @1 :List(Text); }
enum TimeRepresentation { rfc3339 @0; unixMillis @1; unixSeconds @2; }
enum UnknownTimeReason {
  legacyUnprofiled @0; noProfile @1; notJson @2; projectionFailed @3;
  missingField @4; ambiguousField @5; wrongKind @6; invalidInstant @7;
}
struct KnownEventTime {
  instant @0 :Instant; profileId @1 :Text; occurrence @2 :UInt64;
  sourceStart @3 :UInt64; sourceEnd @4 :UInt64;
}
struct UnknownEventTime { reason @0 :UnknownTimeReason; profileId @1 :OptionalText; }
struct EventTime { union { unknown @0 :UnknownEventTime; known @1 :KnownEventTime; } }
struct SourceTimeProfile {
  storeId @0 :Text; id @1 :Text; source @2 :Text; path @3 :Text;
  representation @4 :TimeRepresentation; durability @5 :Durability = durable;
}
struct OptionalSourceTimeProfile { union { absent @0 :Void; value @1 :SourceTimeProfile; } }
struct SourceTimeDefinition { source @0 :Text; path @1 :Text; representation @2 :TimeRepresentation; }
struct EventInterval { from @0 :Instant; before @1 :Instant; }
struct OptionalEventInterval { union { absent @0 :Void; value @1 :EventInterval; } }
enum TimeBasis { receivedAt @0; eventTime @1; }
struct EventCoverage {
  evaluated @0 :UInt64; unknown @1 :UInt64; outsideInterval @2 :UInt64;
  membershipComplete @3 :Bool;
}
struct Pin {
  storeId @0 :Text; id @1 :Text; name @2 :Text; expiresAt @3 :Instant;
  revision @4 :UInt64; members @5 :List(Evidence);
  requestId @6 :Text; durability @7 :Durability = durable;
}
struct Status {
  storeId @0 :Text; storeFormat @1 :Text; physicalBytes @2 :UInt64;
  diskBudget @3 :UInt64; maintenanceReserve @4 :UInt64; liveRecords @5 :UInt64;
  uniquePayloadChunks @6 :UInt64; activePins @7 :UInt64;
  uniqueJsonProjections @8 :UInt64; retryWindowSeconds @9 :UInt64;
  maxArtifactBytes @10 :UInt64; maxRecordsPerArtifact @11 :UInt64; maxPinMembers @12 :UInt64;
  schemaVersion @13 :UInt32 = 1;
  textAnalyzer @14 :Text; textProfile @15 :Text;
  rdfProfile @16 :Text;
  pinReadHandles @17 :UInt64; pinReadBytes @18 :UInt64;
  maxPinReadHandles @19 :UInt64; maxPinReadBytes @20 :UInt64; pinReadLeaseMs @21 :UInt64;
  maxEventBatchBytes @22 :UInt32; maxEventBatchEvents @23 :UInt32;
  durabilityStatus @24 :DurabilityStatus;
  consumeWatches @25 :List(ConsumeWatchStatus);
  pressureStatus @26 :PressureStatus;
}

enum PressurePhase { ready @0; requested @1; awaitingReclamation @2; blocked @3; }
enum EvictionFrontier { exhausted @0; unitLimit @1; scanBudget @2; payloadBudget @3; catalogBudget @4; }
struct EvictionPass {
  records @0 :UInt32; artifacts @1 :UInt32; visited @2 :UInt32; protected @3 :UInt32;
  payloadRefs @4 :UInt32; pendingManifestRefs @5 :UInt64; frontier @6 :EvictionFrontier;
  continuation @7 :Bool; recordlessArtifacts @8 :UInt32;
}
struct PressurePass { union { absent @0 :Void; present @1 :EvictionPass; } }
struct PressureStatus {
  phase @0 :PressurePhase; requestedBytes @1 :UInt64; passes @2 :UInt64;
  evictedRecords @3 :UInt64; payloadRefsRetired @4 :UInt64;
  pendingFlushes @5 :UInt64; pendingCompactions @6 :UInt64; snapshots @7 :UInt64;
  lastPass @8 :PressurePass; failure @9 :OptionalText;
  evictedRecordlessArtifacts @10 :UInt64;
}

enum WatchCoverage { reconciling @0; covered @1; }
struct ConsumeWatchStatus {
  id @0 :Text; root @1 :Text; coverage @2 :WatchCoverage;
  directories @3 :UInt32; candidates @4 :UInt32; deferred @5 :Bool;
  pendingClaims @6 :UInt32; completedClaims @7 :UInt64;
  reconciliations @8 :UInt64; coverageLosses @9 :UInt64;
  failure @10 :OptionalText;
  definition @11 :WatchDefinition; revision @12 :UInt64; enabled @13 :Bool;
}

enum WatchMode { consume @0; tail @1; }
enum WatchReadiness { closeOrIdle @0; }
struct WatchAuthority {
  union { unacknowledged @0 :Void; finishedFilesDeleteAfterDurablePublish @1 :Void; }
}
struct WatchDefinition {
  id @0 :Text; root @1 :Text; source @2 :Text; format @3 :Format;
  quietMs @4 :UInt32 = 30000; maxDirectories @5 :UInt32 = 1024;
  maxCandidates @6 :UInt32 = 4096; recursive @7 :Bool = true;
  mode @8 :WatchMode; readiness @9 :WatchReadiness; authority @10 :WatchAuthority;
}
struct WatchInfo {
  storeId @0 :Text; definition @1 :WatchDefinition; revision @2 :UInt64; enabled @3 :Bool;
}
struct WatchList { storeId @0 :Text; watches @1 :List(WatchInfo); }
struct WatchPut {
  storeId @0 :Text; requestId @1 :Text; expectedRevision @2 :UInt64; definition @3 :WatchDefinition;
}
struct WatchRemove { storeId @0 :Text; requestId @1 :Text; id @2 :Text; expectedRevision @3 :UInt64; }
struct WatchMutation {
  requestId @0 :Text; watch @1 :WatchInfo; durability @2 :Durability = durable;
}
struct FileGeneration {
  device @0 :UInt64; inode @1 :UInt64; byteLength @2 :UInt64;
  modifiedSeconds @3 :Int64; modifiedNanos @4 :UInt32;
  changedSeconds @5 :Int64; changedNanos @6 :UInt32;
}
struct WatchHandoff {
  storeId @0 :Text; requestId @1 :Text; watch @2 :Text; watchRevision @3 :UInt64;
  relativePath @4 :Data; generation @5 :FileGeneration;
}
enum HandoffPhase { reserved @0; claimed @1; published @2; removed @3; }
struct HandoffReceipt {
  request @0 :WatchHandoff; importId @1 :Text; phase @2 :HandoffPhase;
  artifactId @3 :OptionalText; durability @4 :Durability = durable;
}

struct WideInteger { high @0 :UInt64; low @1 :UInt64; }
struct EventField {
  name @0 :Text;
  union {
    text @1 :Text; boolean @2 :Bool; signed @3 :Int64; unsigned @4 :UInt64;
    signed128 @5 :WideInteger; unsigned128 @6 :WideInteger;
    float64Bits @7 :UInt64; bytes @8 :Data; debug @9 :Text; error @10 :Text;
  }
}
enum EventLevel { error @0; warn @1; info @2; debug @3; trace @4; }
enum EventKind { log @0; spanNew @1; spanRecord @2; spanEnter @3; spanExit @4; spanClose @5; spanFollowsFrom @6; }
struct OptionalSpanId { union { absent @0 :Void; value @1 :UInt64; } }
struct EventSpan {
  id @0 :UInt64; parent @1 :OptionalSpanId; name @2 :Text; target @3 :Text;
  fields @4 :List(EventField);
}
struct ProducerEvent {
  sequence @0 :UInt64; eventTime @1 :Instant; kind @2 :EventKind; level @3 :EventLevel;
  name @4 :Text; target @5 :Text; fields @6 :List(EventField);
  context @7 :List(EventSpan); span @8 :OptionalSpanId; followsFrom @9 :OptionalSpanId;
}
struct EventBatch {
  requestId @0 :Text; source @1 :Text; clientInstance @2 :Data;
  events @3 :List(ProducerEvent);
}
struct Ingest {
  requestId @0 :Text; source @1 :Text; format @2 :Format; bytes @3 :Data;
  durability @4 :Durability = durable;
}
struct ReadBytes { evidence @0 :Evidence; offset @1 :UInt64; length @2 :UInt32; }
struct Bytes { storeId @0 :Text; data @1 :Data; }
struct PinRequest {
  requestId @0 :Text; name @1 :Text; targets @2 :List(Evidence); days @3 :UInt32;
}
struct OptionalRevision { union { absent @0 :Void; value @1 :UInt64; } }
struct Renew {
  requestId @0 :Text; storeId @1 :Text; pinId @2 :Text; days @3 :UInt32;
  expectedRevision @4 :OptionalRevision;
}
struct Release { requestId @0 :Text; storeId @1 :Text; pinId @2 :Text; }
struct Released { storeId @0 :Text; requestId @1 :Text; pinId @2 :Text; releasedAt @3 :Instant; }
struct Scope {
  source @0 :OptionalText; receivedFrom @1 :Instant;
  receivedBefore @2 :Instant; limit @3 :UInt32 = 100; scanBudget @4 :UInt32 = 10000;
  selection @5 :QuerySelection;
  sourceIds @6 :OptionalSourceIds;
  diagnostics @7 :List(DiagnosticFilter);
}
struct OptionalSourceIds { union { absent @0 :Void; value @1 :List(Text); } }
struct PinScope { storeId @0 :Text; id @1 :Text; expectedRevision @2 :OptionalRevision; }
struct QuerySelection { union { live @0 :Void; evidence @1 :List(Evidence); pin @2 :PinScope; } }
struct ValueQuery {
  scope @0 :Scope; path @1 :OptionalText; kind @2 :OptionalKind;
  lexical @3 :OptionalText; decoded @4 :OptionalText;
}
enum Execution { complete @0; limited @1; }
enum Certainty { exact @0; incomplete @1; }
struct Coverage {
  visited @0 :UInt64; eligibleRecords @1 :UInt64; failedJsonRecords @2 :UInt64;
  nonJsonRecords @3 :UInt64; decodingFailures @4 :UInt64;
}
struct Receipt {
  storeId @0 :Text; membershipAt @1 :Instant; execution @2 :Execution;
  certainty @3 :Certainty; coverage @4 :Coverage;
}
struct Key { path @0 :Text; kind @1 :JsonKind; occurrences @2 :UInt64; records @3 :UInt64; }
struct Keys { receipt @0 :Receipt; rows @1 :List(Key); }
struct ValueMatch {
  recordId @0 :Text; artifactId @1 :Text; source @2 :Text; receivedAt @3 :Instant;
  sourceStart @4 :UInt64; sourceEnd @5 :UInt64; occurrence @6 :UInt64;
  path @7 :Text; kind @8 :JsonKind; lexical @9 :OptionalText; decoded @10 :OptionalText;
}
struct Values { receipt @0 :Receipt; rows @1 :List(ValueMatch); }
enum TextBasis { rawRecord @0; decodedJsonScalar @1; }
struct SourceRange { start @0 :UInt64; end @1 :UInt64; }
struct TextEvidence {
  term @0 :Text; sourceStart @1 :UInt64; sourceEnd @2 :UInt64;
  basis @3 :TextBasis; coarse @4 :Bool; path @5 :OptionalText;
  contributors @6 :List(SourceRange);
}
struct TextMatch {
  recordId @0 :Text; artifactId @1 :Text; source @2 :Text; receivedAt @3 :Instant;
  score @4 :Text; evidence @5 :List(TextEvidence); evidenceLimited @6 :Bool;
}
struct TextRanking {
  profile @0 :Text; corpusProfile @1 :Text; analyzer @2 :Text;
  documents @3 :UInt64; tokens @4 :UInt64; membershipComplete @5 :Bool; terms @6 :List(Text);
  corpusId @7 :Text;
}
struct TextCoverage { failedOrIncomplete @0 :UInt64; notApplicable @1 :UInt64; postingVisits @2 :UInt64; }
struct TextQuery { scope @0 :Scope; text @1 :Text; }
struct TextResults { receipt @0 :Receipt; ranking @1 :TextRanking; coverage @2 :TextCoverage; rows @3 :List(TextMatch); }
enum ContextOrder { receivedAtRecordId @0; }
enum ContextFrontier { requestedCount @0; scopeBoundary @1; scanBudget @2; }
struct ContextQuery {
  anchor @0 :RecordRef; scope @1 :Scope; ordering @2 :ContextOrder;
  before @3 :UInt32 = 10; after @4 :UInt32 = 10;
}
struct ContextSide {
  rows @0 :List(Record); visited @1 :UInt64; retired @2 :UInt64;
  frontier @3 :ContextFrontier;
}
struct Context {
  receipt @0 :Receipt; anchor @1 :Record; before @2 :ContextSide; after @3 :ContextSide;
  ordering @4 :ContextOrder; continuityUnknown @5 :Bool = true;
}
enum ProjectionState { complete @0; failed @1; notApplicable @2; }
struct RdfInspection {
  storeId @0 :Text; recordId @1 :Text; membershipAt @2 :Instant;
  metadataGraph @3 :Text; metadataPrimaryRows @4 :UInt64;
  state @5 :ProjectionState; contentGraph @6 :OptionalText; diagnostic @7 :OptionalText;
  primaryRows @8 :UInt64; reifierRows @9 :UInt64; annotationRows @10 :UInt64;
  diagnosticGraph @11 :OptionalText; diagnosticPrimaryRows @12 :UInt64;
}
struct PinSelector { union { active @0 :Void; id @1 :Text; name @2 :Text; } }
struct PinReadQuery {
  storeId @0 :Text; selector @1 :PinSelector; cursor @2 :OptionalText;
  pageSize @3 :UInt32 = 200; scanBudget @4 :UInt32 = 10000;
}
struct PinSummary {
  id @0 :Text; name @1 :Text; expiresAt @2 :Instant;
  revision @3 :UInt64; directMemberCount @4 :UInt64;
}
struct PinReadReceipt {
  storeId @0 :Text; snapshotId @1 :Text; asOf @2 :Instant; leaseExpiresAt @3 :Instant;
  totalItems @4 :UInt64; pageStart @5 :UInt64; nextCursor @6 :OptionalText;
  pageSize @7 :UInt32; scanBudget @8 :UInt32; scanned @9 :UInt64;
}
struct PinMembers { pin @0 :PinSummary; members @1 :List(Evidence); }
struct PinReadPage {
  receipt @0 :PinReadReceipt;
  union { collections @1 :List(PinSummary); members @2 :PinMembers; }
}
struct JsonPredicate {
  path @0 :OptionalText; kind @1 :OptionalKind;
  lexical @2 :OptionalText; decoded @3 :OptionalText;
}
struct FieldConstraint {
  predicate @0 :JsonPredicate; parentBinding @1 :OptionalText; parentPath @2 :OptionalText;
}
struct RecordQuery {
  scope @0 :Scope; text @1 :OptionalText; fields @2 :List(FieldConstraint);
  requireCompleteIndex @3 :Bool;
  eventInterval @4 :OptionalEventInterval;
}
struct OptionalOccurrence { union { absent @0 :Void; value @1 :UInt64; } }
struct FieldEvidence {
  predicateIndex @0 :UInt32; occurrence @1 :UInt64; path @2 :Text; kind @3 :JsonKind;
  sourceStart @4 :UInt64; sourceEnd @5 :UInt64;
  parentBinding @6 :OptionalText; boundParent @7 :OptionalOccurrence;
}
struct RecordMatch {
  recordId @0 :Text; artifactId @1 :Text; source @2 :Text; receivedAt @3 :Instant;
  byteStart @4 :UInt64; byteEnd @5 :UInt64; fields @6 :List(FieldEvidence);
  eventTime @7 :EventTime;
  diagnostics @8 :DiagnosticSnapshot;
}
struct OptionalTextResults { union { absent @0 :Void; value @1 :TextResults; } }
struct RecordsResults {
  receipt @0 :Receipt; rows @1 :List(RecordMatch); text @2 :OptionalTextResults;
  fieldVisits @3 :UInt64; qualifiedRecords @4 :UInt64;
  timeBasis @5 :TimeBasis; eventCoverage @6 :EventCoverage;
}
# Private upload progress is distinct from accepted evidence.
struct ImportRef { storeId @0 :Text; id @1 :Text; }
struct ImportBegin {
  requestId @0 :Text; source @1 :Text; format @2 :Format;
  byteLength @3 :UInt64; payloadId @4 :Text;
}
struct ImportProgress {
  storeId @0 :Text; id @1 :Text; requestId @2 :Text; source @3 :Text;
  format @4 :Format; byteLength @5 :UInt64; payloadId @6 :Text;
  receivedBytes @7 :UInt64; expiresAt @8 :Instant; sealed @9 :Bool;
  layoutComplete @10 :Bool; layoutRecords @11 :UInt64;
  publishedArtifact @12 :OptionalText;
}
struct ImportStart { union { staging @0 :ImportProgress; accepted @1 :Artifact; } }
struct ImportChunk { import @0 :ImportRef; offset @1 :UInt64; bytes @2 :Data; }
struct ImportPrepare { import @0 :ImportRef; expectedRecord @1 :UInt64; }
struct ImportPreparation {
  storeId @0 :Text; importId @1 :Text; reservedArtifactId @2 :Text;
  preparedRecords @3 :UInt64; totalRecords @4 :UInt64;
  receivedAt @5 :Instant; expiresAt @6 :Instant; sourceTimeProfile @7 :OptionalText;
  raw @8 :OptionalRawPreparation;
  diagnosticProfile @9 :OptionalText;
}
struct RawPreparation {
  recordIndex @0 :UInt64; reservedRecordId @1 :Text;
  preparedChunks @2 :UInt64; totalChunks @3 :UInt64;
}
struct OptionalRawPreparation { union { absent @0 :Void; value @1 :RawPreparation; } }
struct ImportCanceled { import @0 :ImportRef; durability @1 :Durability = durable; }

interface Sparrow {
  status @0 () -> (result :Result(Status));
  ingest @1 (request :Ingest) -> (result :Result(Artifact));
  artifactGet @2 (artifact :ArtifactRef) -> (result :Result(Artifact));
  recordGet @3 (record :RecordRef) -> (result :Result(Record));
  bytesRead @4 (request :ReadBytes) -> (result :Result(Bytes));
  pin @5 (request :PinRequest) -> (result :Result(Pin));
  pinRelease @6 (request :Release) -> (result :Result(Released));
  keysList @7 (scope :Scope) -> (result :Result(Keys));
  valuesSearch @8 (query :ValueQuery) -> (result :Result(Values));
  textSearch @9 (query :TextQuery) -> (result :Result(TextResults));
  recordsContext @10 (query :ContextQuery) -> (result :Result(Context));
  rdfInspect @11 (record :RecordRef) -> (result :Result(RdfInspection));
  pinRenew @12 (request :Renew) -> (result :Result(Pin));
  pinRead @13 (query :PinReadQuery) -> (result :Result(PinReadPage));
  recordsSearch @14 (query :RecordQuery) -> (result :Result(RecordsResults));
  sourceTimeDefine @15 (definition :SourceTimeDefinition) -> (result :Result(SourceTimeProfile));
  sourceTimeGet @16 (source :Text) -> (result :Result(OptionalSourceTimeProfile));
  importBegin @17 (request :ImportBegin) -> (result :Result(ImportStart));
  importAppend @18 (request :ImportChunk) -> (result :Result(ImportProgress));
  importGet @19 (import :ImportRef) -> (result :Result(ImportProgress));
  importSeal @20 (import :ImportRef) -> (result :Result(ImportProgress));
  importLayout @21 (import :ImportRef) -> (result :Result(ImportProgress));
  importPrepare @22 (request :ImportPrepare) -> (result :Result(ImportPreparation));
  importPublish @23 (import :ImportRef) -> (result :Result(Artifact));
  importCancel @24 (import :ImportRef) -> (result :Result(ImportCanceled));
  eventsIngest @25 (batch :EventBatch, durability :Durability = durable) -> (result :Result(Artifact));
  flush @26 (through :DurabilityWatermark) -> (result :Result(DurabilityWatermark));
  watchList @27 () -> (result :Result(WatchList));
  watchPut @28 (request :WatchPut) -> (result :Result(WatchMutation));
  watchRemove @29 (request :WatchRemove) -> (result :Result(WatchMutation));
  watchHandoff @30 (request :WatchHandoff) -> (result :Result(HandoffReceipt));
  watchHandoffGet @31 (request :WatchHandoff) -> (result :Result(HandoffReceipt));
  diagnosticDefine @32 (definition :DiagnosticDefinition) -> (result :Result(DiagnosticProfile));
  diagnosticGet @33 (source :Text) -> (result :Result(OptionalDiagnosticProfile));
}
