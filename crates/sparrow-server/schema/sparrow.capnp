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
enum JsonKind { string @0; number @1; booleanTrue @2; booleanFalse @3; null @4; array @5; object @6; }
struct OptionalKind { union { absent @0 :Void; value @1 :JsonKind; } }
struct Artifact {
  storeId @0 :Text; id @1 :Text; source @2 :Text; format @3 :Format;
  receivedAt @4 :Instant; expiresAt @5 :Instant; byteLength @6 :UInt64;
  payloadId @7 :Text; recordIds @8 :List(Text);
  durability @9 :Durability = durable;
}
struct ArtifactRef { storeId @0 :Text; id @1 :Text; }
struct RecordRef { storeId @0 :Text; id @1 :Text; }
struct Record {
  storeId @0 :Text; id @1 :Text; artifactId @2 :Text; source @3 :Text;
  receivedAt @4 :Instant; expiresAt @5 :Instant;
  byteStart @6 :UInt64; byteEnd @7 :UInt64;
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
}
struct Ingest {
  requestId @0 :Text; source @1 :Text; format @2 :Format; bytes @3 :Data;
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
}
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
}
