@0xdcc9fefce9188f9a;
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

struct Instant { unixMillis @0 :UInt64; }
# `class` (additive, #257) says why a retryable refusal was made. `unclassified` is every refusal
# an older daemon sends. `freeSpaceFloor` is the free-space floor: the code stays
# STORAGE_PRESSURE and `retryable` stays true, so a client that ignores `class` holds and retries
# as it always has; one that reads it knows the producer must hold until space returns.
enum FailureClass { unclassified @0; freeSpaceFloor @1; }
struct Failure { code @0 :Text; message @1 :Text; retryable @2 :Bool; class @3 :FailureClass; }
struct Result(T) { union { ok @0 :T; error @1 :Failure; } }
struct OptionalText { union { absent @0 :Void; value @1 :Text; } }
struct Evidence { storeId @0 :Text; union { artifact @1 :Text; record @2 :Text; } }
enum Format { jsonl @0; log @1; json @2; markdown @3; opaque @4; syslog @5; }
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
  # Schema 68: endpoint and caller identity, present only for ingest-socket evidence.
  intake @11 :OptionalIntake;
}
enum IntakeEndpoint { ingestSocket @0; }
# The SO_PEERCRED identity the ingest socket read when it accepted the connection.
struct Intake { endpoint @0 :IntakeEndpoint; uid @1 :UInt32; gid @2 :UInt32; pid @3 :UInt32; }
struct OptionalIntake { union { absent @0 :Void; value @1 :Intake; } }
struct ArtifactRef { storeId @0 :Text; id @1 :Text; }
struct ArtifactOrigin {
  format @0 :Format; receivedAt @1 :Instant; expiresAt @2 :Instant;
  byteLength @3 :UInt64; originalRecords @4 :UInt64;
}
struct OptionalArtifactOrigin { union { absent @0 :Void; value @1 :ArtifactOrigin; } }
struct ArtifactSummary {
  id @0 :Text; source @1 :Text; origin @2 :OptionalArtifactOrigin;
  completeBytesAvailable @3 :Bool; retainedRecords @4 :UInt64;
  coverage @5 :DiagnosticCoverage; matchingRecords @6 :Count;
}
struct ArtifactReceiptInterval { from @0 :Instant; before @1 :Instant; }
struct OptionalArtifactReceiptInterval {
  union { unfiltered @0 :Void; interval @1 :ArtifactReceiptInterval; }
}
struct ArtifactListQuery {
  storeId @0 :Text; schemaVersion @1 :UInt32; source @2 :OptionalText;
  received @3 :OptionalArtifactReceiptInterval;
  cursor @4 :OptionalText; pageSize @5 :UInt32 = 200; scanBudget @6 :UInt32 = 10000;
  scope @7 :Scope;
}
struct ArtifactListPage {
  receipt @0 :CatalogReadReceipt; rows @1 :List(ArtifactSummary);
  coverage @2 :DiagnosticCoverage; matchingRecords @3 :Count;
  # Schema 66: a live receipt-ordered page; totalItems counts rows delivered so far.
  receiptWalk @4 :Bool;
}
struct SourceWatchInfo { watchId @0 :Text; revision @1 :UInt64; enabled @2 :Bool; }
struct SourceSummary {
  sourceId @0 :Text; rdfIri @1 :Text;
  completeArtifacts @2 :UInt64; fragmentParents @3 :UInt64; retainedRecords @4 :UInt64;
  timeProfileId @5 :OptionalText; diagnosticProfileId @6 :OptionalText;
  continuityUnknown @7 :Bool; watches @8 :List(SourceWatchInfo);
  producerStreamIds @9 :List(Text);
  tailGenerationIds @10 :List(Text);
  syslogListenerIds @11 :List(Text);
  receiverWindowIds @12 :List(Text);
}
struct SourceListQuery {
  storeId @0 :Text; schemaVersion @1 :UInt32; source @2 :OptionalText;
  cursor @3 :OptionalText; pageSize @4 :UInt32 = 200; scanBudget @5 :UInt32 = 10000;
}
struct SourceListPage { receipt @0 :CatalogReadReceipt; rows @1 :List(SourceSummary); }
struct RecordRef { storeId @0 :Text; id @1 :Text; }
struct Record {
  storeId @0 :Text; id @1 :Text; artifactId @2 :Text; source @3 :Text;
  receivedAt @4 :Instant; expiresAt @5 :Instant;
  byteStart @6 :UInt64; byteEnd @7 :UInt64;
  eventTime @8 :EventTime;
  diagnostics @9 :DiagnosticSnapshot;
  producer @10 :OptionalProducerPosition;
  syslog @11 :OptionalSyslogSnapshot;
  tail @12 :OptionalTailPosition;
  receiver @13 :OptionalReceiverPosition;
  intake @14 :OptionalIntake;
}
struct TailDeclaration {
  requestId @0 :Text; watch @1 :Text; watchRevision @2 :UInt64;
  relativePath @3 :Data; source @4 :Text; format @5 :Format;
  device @6 :UInt64; inode @7 :UInt64;
  rootDevice @8 :UInt64; rootInode @9 :UInt64;
  parentDevice @10 :UInt64; parentInode @11 :UInt64;
}
struct TailPosition {
  generationId @0 :Text; declaration @1 :TailDeclaration;
  sourceStart @2 :UInt64; sourceEnd @3 :UInt64;
}
struct OptionalTailPosition { union { absent @0 :Void; value @1 :TailPosition; } }
struct OptionalInstant { union { absent @0 :Void; value @1 :Instant; } }
struct TailGeneration {
  storeId @0 :Text; id @1 :Text; declaration @2 :TailDeclaration;
  startedAt @3 :Instant; offset @4 :UInt64; records @5 :UInt64;
  complete @6 :Bool; gap @7 :OptionalText; replacementEligibleAt @8 :OptionalInstant;
  continuityUnknown @9 :Bool = true;
  finish @10 :OptionalTailFinish;
}
struct TailFinishIntent {
  storeId @0 :Text; requestId @1 :Text; generationId @2 :Text;
  watchRevision @3 :UInt64; observed @4 :FileGeneration;
}
struct TailFinish {
  request @0 :TailFinishIntent; admittedAt @1 :Instant; finishedAt @2 :OptionalInstant;
}
struct OptionalTailFinish { union { absent @0 :Void; value @1 :TailFinish; } }
struct TailGenerationQuery { storeId @0 :Text; schemaVersion @1 :UInt32; id @2 :Text; }
enum SyslogDialect { rfc5424 @0; rfc3164 @1; }
enum SyslogHeader { timestamp @0; host @1; app @2; procid @3; msgid @4; tag @5; }
struct SyslogField { kind @0 :SyslogHeader; value @1 :Text; sourceStart @2 :UInt64; sourceEnd @3 :UInt64; }
struct SyslogParameter { name @0 :Text; valueStart @1 :UInt64; valueEnd @2 :UInt64; }
struct SyslogElement { id @0 :Text; sourceStart @1 :UInt64; sourceEnd @2 :UInt64; parameters @3 :List(SyslogParameter); }
struct ParsedSyslog {
  dialect @0 :SyslogDialect; priority @1 :UInt16; priorityStart @2 :UInt64; priorityEnd @3 :UInt64;
  fields @4 :List(SyslogField); structuredData @5 :List(SyslogElement); messageStart @6 :UInt64;
}
struct SyslogSnapshot { profileId @0 :Text; union { parsed @1 :ParsedSyslog; failed @2 :Text; } }
struct OptionalSyslogSnapshot { union { absent @0 :Void; value @1 :SyslogSnapshot; } }
enum SyslogTransport { tcp @0; udp @1; }
enum SyslogFraming { octetCounting @0; lf @1; crLf @2; }
struct OptionalSyslogFraming { union { absent @0 :Void; value @1 :SyslogFraming; } }
struct SyslogListenerStatus {
  id @0 :Text; source @1 :Text; address @2 :Text; transport @3 :SyslogTransport;
  framing @4 :OptionalSyslogFraming; maxMessageBytes @5 :UInt32; maxConnections @6 :UInt32; readTimeoutMs @7 :UInt32;
  activeConnections @8 :UInt32; acceptedMessages @9 :UInt64; interpretationFailures @10 :UInt64;
  refusedMessages @11 :UInt64; truncatedDatagrams @12 :UInt64; framingFailures @13 :UInt64;
  rejectedConnections @14 :UInt64; timedOutConnections @15 :UInt64;
  continuityUnknown @16 :Bool; kernelDropsUnknown @17 :Bool; failure @18 :OptionalText;
  # TCP frames held under pressure (#243): the frames held now, frames ever held per reason, and
  # the longest hold, finished or running, in milliseconds.
  heldFrames @19 :UInt32; holdsStoragePressure @20 :UInt64; holdsNoPermit @21 :UInt64;
  longestHoldMs @22 :UInt64;
  # UDP kernel evidence (#254): the receive buffer asked for and the size getsockopt reports back
  # (Linux doubles the request and clamps it to net.core.rmem_max), the socket's kernel drop count
  # (all causes), and whether that count could be read. A count is meaningful only when
  # kernelCounterState is available; otherwise kernelDrops is zero and kernelCounterReason says why.
  rcvbufRequestedBytes @23 :UInt64; rcvbufGrantedBytes @24 :UInt64; kernelDrops @25 :UInt64;
  kernelCounterState @26 :KernelCounterState; kernelCounterReason @27 :OptionalText;
  # True once the socket has closed: kernelDrops is then the final count, taken just before.
  kernelCounterFinal @28 :Bool;
  # The kernel refused the enlarge request: the socket kept the default buffer (granted shows it)
  # and the daemon went on taking syslog. The text is the error; the count is 0 or 1 per listener.
  rcvbufEnlargeError @29 :OptionalText; rcvbufEnlargeFailures @30 :UInt64;
  # UDP hold (#255): the reader and the publisher are joined by a FIFO bounded in bytes. The
  # datagrams and bytes queued now (entry cost is length plus a fixed overhead), the configured
  # bound, the times the reader stopped because the queue was full and the longest such wait
  # (finished or running) in milliseconds, datagrams published with the commit instant because
  # the read instant was behind the archive watermark, and datagrams read but not stored when
  # the listener stopped under pressure. heldFrames, holdsStoragePressure, holdsNoPermit and
  # longestHoldMs count the datagram at the head of the queue for a UDP listener.
  queuedDatagrams @31 :UInt32; queuedBytes @32 :UInt64; holdBytes @33 :UInt64;
  readerFullWaits @34 :UInt64; readerFullLongestMs @35 :UInt64; restamped @36 :UInt64;
  lostAtShutdown @37 :UInt64; lostAtShutdownBytes @38 :UInt64;
  # Datagrams whose read instant and commit instant were both at or behind the archive watermark
  # (the clock stepped back): stored with the instant just past the watermark, so their
  # received_at is that fence-adjusted time and not the clock.
  restampedPastWatermark @39 :UInt64;
}
enum KernelCounterState { notApplicable @0; available @1; unavailable @2; }
struct SyslogListenerQuery { storeId @0 :Text; schemaVersion @1 :UInt32; source @2 :Text; id @3 :Text; }
struct ProducerPosition { streamId @0 :Text; sequence @1 :UInt64; }
struct OptionalProducerPosition { union { absent @0 :Void; value @1 :ProducerPosition; } }
struct ProducerGap { first @0 :UInt64; last @1 :UInt64; }
struct ProducerStream {
  storeId @0 :Text; id @1 :Text; source @2 :Text; instance @3 :Data;
  firstSequence @4 :UInt64; lastSequence @5 :UInt64; acceptedEvents @6 :UInt64;
  unobservedSequences @7 :UInt64; gaps @8 :List(ProducerGap); gapsComplete @9 :Bool;
  firstReceivedAt @10 :Instant; lastReceivedAt @11 :Instant; expiresAt @12 :Instant;
  continuityUnknown @13 :Bool;
}
struct ProducerStreamQuery { storeId @0 :Text; schemaVersion @1 :UInt32; id @2 :Text; }
struct ReceiverPosition { windowId @0 :Text; sequence @1 :UInt64; }
struct OptionalReceiverPosition { union { absent @0 :Void; value @1 :ReceiverPosition; } }
enum ReceiverTransport { udp @0; tcpOctetCounting @1; tcpLf @2; tcpCrLf @3; }
struct ReceiverWindow {
  storeId @0 :Text; id @1 :Text; source @2 :Text; listener @3 :Text; instance @4 :Text;
  transport @5 :ReceiverTransport; observations @6 :UInt64; accepted @7 :UInt64; discarded @8 :UInt64;
  refused @9 :UInt64; truncated @10 :UInt64; framingFailures @11 :UInt64; timedOut @12 :UInt64;
  gaps @13 :List(ProducerGap); gapsComplete @14 :Bool;
  firstReceivedAt @15 :Instant; lastReceivedAt @16 :Instant; expiresAt @17 :Instant;
  continuityUnknown @18 :Bool; preCommitLossUnknown @19 :Bool;
}
struct ReceiverWindowQuery { storeId @0 :Text; schemaVersion @1 :UInt32; id @2 :Text; }
struct ProducerLimits {
  maxStreams @0 :UInt32; maxGapIntervals @1 :UInt32; maxRegistryBytes @2 :UInt64;
  metadataRetentionSeconds @3 :UInt64;
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
  syslogMalformed @8; syslogLegacyTime @9; syslogNilTime @10; syslogSubmillisecond @11;
  logInputUnavailable @12; logInvalidUtf8 @13; logNoMatch @14; logCaptureMissing @15; logWrongFormat @16;
}
struct KnownEventTime {
  instant @0 :Instant; profileId @1 :Text; occurrence @2 :UInt64;
  sourceStart @3 :UInt64; sourceEnd @4 :UInt64;
}
# Schema 69: an unknown source time is placed on the event timeline at the record's
# receipt instant, marked as a receipt-basis fallback. The stored reason is unchanged.
struct UnknownEventTime {
  reason @0 :UnknownTimeReason; profileId @1 :OptionalText;
  fallback @2 :Instant;
}
struct EventTime { union { unknown @0 :UnknownEventTime; known @1 :KnownEventTime; } }
struct SourceTimeProfile {
  storeId @0 :Text; id @1 :Text; source @2 :Text; path @3 :Text;
  representation @4 :TimeRepresentation; durability @5 :Durability = durable;
  extraction @6 :TimeExtraction;
}
struct LogTimeCapture { pattern @0 :Text; group @1 :UInt32; flags @2 :Text; }
struct TimeExtraction { union { jsonPointer @0 :Void; logCapture @1 :LogTimeCapture; } }
struct OptionalSourceTimeProfile { union { absent @0 :Void; value @1 :SourceTimeProfile; } }
struct SourceTimeDefinition { source @0 :Text; path @1 :Text; representation @2 :TimeRepresentation; extraction @3 :TimeExtraction; schemaVersion @4 :UInt32; }
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
  maxInlineArtifactBytes @10 :UInt64; maxRecordsPerArtifact @11 :UInt64; maxPinMembers @12 :UInt64;
  schemaVersion @13 :UInt32 = 1;
  textAnalyzer @14 :Text; textProfile @15 :Text;
  rdfProfile @16 :Text;
  pinReadHandles @17 :UInt64; pinReadBytes @18 :UInt64;
  maxPinReadHandles @19 :UInt64; maxPinReadBytes @20 :UInt64; pinReadLeaseMs @21 :UInt64;
  maxEventBatchBytes @22 :UInt32; maxEventBatchEvents @23 :UInt32;
  durabilityStatus @24 :DurabilityStatus;
  consumeWatches @25 :List(ConsumeWatchStatus);
  pressureStatus @26 :PressureStatus;
  artifactReadHandles @27 :UInt64; artifactReadBytes @28 :UInt64;
  maxArtifactReadHandles @29 :UInt64; maxArtifactReadBytes @30 :UInt64;
  artifactReadLeaseMs @31 :UInt64;
  sourceReadHandles @32 :UInt64; sourceReadBytes @33 :UInt64;
  maxSourceReadHandles @34 :UInt64; maxSourceReadBytes @35 :UInt64;
  sourceReadLeaseMs @36 :UInt64;
  recordReadHandles @37 :UInt64; recordReadBytes @38 :UInt64;
  maxRecordReadHandles @39 :UInt64; maxRecordReadBytes @40 :UInt64;
  recordReadLeaseMs @41 :UInt64;
  producerLimits @42 :ProducerLimits;
  syslogListeners @43 :List(SyslogListenerStatus);
  sparqlReadHandles @44 :UInt64; sparqlReadBytes @45 :UInt64;
  maxSparqlReadHandles @46 :UInt64; maxSparqlReadBytes @47 :UInt64;
  sparqlReadLeaseMs @48 :UInt64;
  recompression @49 :RecompressionStatus;
  queryOnly @50 :Bool; retentionSeconds @51 :UInt64;
  # Schema 68: the group-accessible ingest-only Unix socket, when configured.
  ingestSocket @52 :OptionalIngestSocketStatus;
  # Schema 69: the online store migrations and their progress.
  migrations @53 :List(MigrationStatus);
  # Schema 70: true while the store counters seed. liveRecords, uniquePayloadChunks and
  # uniqueJsonProjections then count only the keys through the `store-counters` watermark;
  # they are not totals until this is false.
  countsPending @54 :Bool;
  # Schema 70: the online backup (#156), removed in #330. The ordinal stays reserved; the daemon
  # always reports phase `unconfigured`, an empty root and no failure.
  backup @55 :BackupStatus;
  # Schema 71 (additive, #215): gap evidence lost to a full producer-stream or receiver-window
  # registry since the daemon started. The logs were taken; only the stream's window is absent.
  streamEvidenceLoss @56 :StreamEvidenceLoss;
  # Schema 71 (additive, #224): files tailed without a stored checkpoint because the generation
  # registry was full and no finished row could be retired, and rows retired for room.
  tailRegistryEvidence @57 :TailRegistryEvidence;
  # Schema 71 (additive, #227): records stored with unknown time because the source mapping
  # registry (1024 sources) was full, per source, since the daemon started. Every row has
  # kind `sourceTime` and an empty instance; `events` counts the records. A reader that does
  # not know this field sees no change.
  sourceTimeLoss @58 :StreamEvidenceLoss;
  # Schema 71 (additive, #246): permit, import-slot and connection-ceiling waits. A reader that
  # does not know this field sees no change.
  admission @59 :AdmissionStatus;
  # Schema 71 (additive, #257): what the daemon did while the free-space floor paused writes.
  # A reader that does not know this field sees no change.
  floorPause @60 :FloorPauseStatus;
  # Additive (#280): declarations refused above the import bound since the daemon started. It
  # names a caller that does not split its input. A reader that does not know this field sees
  # no change.
  importOverBoundRefusals @61 :UInt64;
  # Additive (#340): graphs refused because their RDF would exceed a staging limit, and the
  # latest one. One refused intake can add several, see `StagingRefusalStatus`. A reader that
  # does not know this field sees no change.
  stagingRefusals @62 :StagingRefusalStatus;
  # Additive (#324): times an archive prepare halved its range after an encode error since the
  # daemon started. In memory, so it restarts at zero. A reader that does not know this field
  # sees no change.
  archiveHalvings @63 :UInt64;
  # Additive (#365): the archive worker's phase, durable watermark and last failure, as the
  # worker last published them. A reader that does not know this field sees no change.
  archiveWorker @64 :ArchiveWorkerStatus;
}
# The archive worker as of the end of its last step. `through` is the durable `archive:through`
# watermark, empty before the first commit and read from the store when the worker starts; `intentInFlight` is true while a prepared segment
# is recorded and not yet committed. `failure` is the last failed pass and is absent once a
# pass ends without error. `holding` is the phase between a failed pass and the next one.
enum ArchivePhase { notConfigured @0; idle @1; preparing @2; writing @3; verifying @4; holding @5; }
struct ArchiveWorkerStatus {
  phase @0 :ArchivePhase; through @1 :Text; intentInFlight @2 :Bool;
  failure @3 :OptionalArchiveFailure;
}
# `code` is the error class (`ARCHIVE_VERIFY`, `ARCHIVE_IO`, `STORAGE_PRESSURE`, ...); `segment`
# is the segment's file name, empty when the failure came before a segment was chosen; `reason`
# is the cause, cut to a bounded length; `since` is the instant the failure began, kept across
# repeated passes with the same code and segment, and absent while the clock could not be read.
struct ArchiveFailure { code @0 :Text; segment @1 :Text; reason @2 :Text; since @3 :OptionalInstant; }
struct OptionalArchiveFailure { union { absent @0 :Void; value @1 :ArchiveFailure; } }
# RDF staging refusals since the daemon started: `refusals` counts every refused graph, not
# refused intakes. A projection graph an ingest absorbs into a failed projection counts, and the
# batch can still fail afterwards on a later graph, which counts again. The `last*` fields name the latest: the limit
# (`write` or `dictionary`), the artifact's source, the graph identity and the bytes anticipated.
# All are empty or 0 before the first refusal.
struct StagingRefusalStatus {
  refusals @0 :UInt64; lastLimit @1 :Text; lastSource @2 :Text; lastGraph @3 :Text; lastBytes @4 :UInt64;
}
# Connections the daemon kept accepting while paused, writes it answered with the floor label,
# reads it served while paused and reads refused for staging, since it started. `pausedSince` is the
# Unix milliseconds the current pause began, 0 when not paused.
struct FloorPauseStatus {
  pausedSince @0 :UInt64; connectionsAccepted @1 :UInt64; writesRefused @2 :UInt64;
  readsServed @3 :UInt64; readStagingRefused @4 :UInt64;
}
# One kind of wait (#246): acquisitions that found nothing free and waited, waits in progress,
# the longest wait (finished or running) in milliseconds, and waits that ended at their bound.
struct WaitStatus { waits @0 :UInt64; waiting @1 :UInt32; longestWaitMs @2 :UInt64; timeouts @3 :UInt64; }
# A connection ceiling that holds peers in the kernel backlog instead of resetting them. `ceilingWait`
# counts the times the accept loop found every slot taken and paused for one; `leaseExpiries`
# counts connections closed by the 60-second lease.
struct ConnectionCeilingStatus {
  maxConnections @0 :UInt32; activeConnections @1 :UInt32;
  ceilingWait @2 :WaitStatus; leaseExpiries @3 :UInt64;
}
struct OptionalConnectionCeilingStatus { union { absent @0 :Void; value @1 :ConnectionCeilingStatus; } }
struct AdmissionStatus {
  # Operation permits no reader may hold, permits readers may hold, and how long a writer waits.
  writerReserve @0 :UInt32; readerSlots @1 :UInt32; writerWaitLimitMs @2 :UInt32;
  writerPermitWait @3 :WaitStatus;
  # Reads answered RESOURCE_LIMIT at once (a reader never waits).
  readerRefusals @4 :UInt64;
  importSlotWait @5 :WaitStatus; importSlotWaitLimitMs @6 :UInt32;
  # The shared ceiling of the private Unix socket and public TCP, absent in a test harness.
  rpcConnections @7 :OptionalConnectionCeilingStatus;
}
# A full tail generation registry never refuses a file. A file tailed without a stored
# checkpoint is read again from its start after a restart, so its `records` and `bytes` are at
# risk of duplication. Counts cover the life of the daemon. The files with the most bytes are
# named, at most `maxNamed`; the rest fold into `other*`. `otherFilesExact` is false when more
# files exist than the daemon tracks, so `otherFiles` is then a lower bound.
struct UnrecordedTailFile {
  watch @0 :Text; path @1 :Text; source @2 :Text;
  generations @3 :UInt64; records @4 :UInt64; bytes @5 :UInt64;
}
struct TailRegistryEvidence {
  named @0 :List(UnrecordedTailFile); maxNamed @1 :UInt32;
  otherFiles @2 :UInt64; otherGenerations @3 :UInt64; otherRecords @4 :UInt64;
  otherBytes @5 :UInt64; otherFilesExact @6 :Bool;
  totalGenerations @7 :UInt64; totalRecords @8 :UInt64; totalBytes @9 :UInt64;
  # Finished rows retired to make room (stored rows retired from the registry and in-memory
  # terminal rows evicted above their bound), and how many of those were complete generations,
  # whose file, if still present, is read again from its start.
  retiredTerminal @10 :UInt64; retiredDuplicateRisk @11 :UInt64;
}
# The streams with the most lost positions are named, at most `maxNamed`; every other stream
# folds into the `other*` totals. `otherStreamsExact` is false when more streams exist than the
# daemon tracks, so `otherStreams` is then a lower bound. The totals cover every stream.
enum LostStreamKind { producer @0; receiver @1; sourceTime @2; }
struct LostStream {
  kind @0 :LostStreamKind; source @1 :Text; instance @2 :Text;
  batches @3 :UInt64; events @4 :UInt64;
}
struct StreamEvidenceLoss {
  named @0 :List(LostStream); maxNamed @1 :UInt32;
  otherStreams @2 :UInt64; otherBatches @3 :UInt64; otherEvents @4 :UInt64;
  otherStreamsExact @5 :Bool;
  totalBatches @6 :UInt64; totalEvents @7 :UInt64;
}
# The removed online backup (#156, #330); kept only for `Status.backup` and the `backup` method.
enum BackupPhase { unconfigured @0; idle @1; running @2; complete @3; failed @4; }
struct BackupStatus {
  phase @0 :BackupPhase; root @1 :Text; keep @2 :UInt32;
  startedAt @3 :UInt64; finishedAt @4 :UInt64;
  backupId @5 :UInt32; files @6 :UInt32; bytes @7 :UInt64;
  failure @8 :OptionalText;
}
# One online store migration. `done` of `total` source entries are migrated; `watermark` is
# the opaque last committed position, `start` before the first page and absent only once
# complete; `failure` is the latched error of the last page, cleared when a page succeeds.
# Schema 70: `totalPending` is true while the store counters seed; `total` is then 0 and
# `done` is exact through the seeding watermark.
struct MigrationStatus {
  name @0 :Text; done @1 :UInt64; total @2 :UInt64;
  watermark @3 :OptionalText; failure @4 :OptionalText;
  totalPending @5 :Bool;
}
struct IngestSocketStatus {
  path @0 :Text; gid @1 :UInt32; maxConnections @2 :UInt32;
  activeConnections @3 :UInt32; refusedConnections @4 :UInt64;
  # Schema 71 (additive, #246): over-ceiling peers wait in the backlog; `refusedConnections`
  # now counts only a peer the kernel cannot name.
  ceilingWait @5 :WaitStatus; leaseExpiries @6 :UInt64;
}
struct OptionalIngestSocketStatus { union { absent @0 :Void; value @1 :IngestSocketStatus; } }

# Constant-size compatibility and admission metadata; no evidence catalog counts.
struct Discovery {
  storeId @0 :Text; schemaVersion @1 :UInt32;
  maxInlineArtifactBytes @2 :UInt64; maxRecordsPerArtifact @3 :UInt64;
  textAnalyzer @4 :Text; textProfile @5 :Text; rdfProfile @6 :Text;
  maxEventBatchBytes @7 :UInt32; maxEventBatchEvents @8 :UInt32;
  durabilityStatus @9 :DurabilityStatus;
  producerLimits @10 :ProducerLimits;
  # Additive (#280): the largest declared import; a larger declaration is refused
  # INVALID_ARGUMENT, so split into parts of at most this many bytes. 0 from an older daemon.
  maxImportBytes @11 :UInt64;
  # Additive (#280): the longest record a staged layout keeps whole; a longer record is laid
  # out as raw pieces. 0 from an older daemon, which a client treats as "no split".
  rawRecordBytes @12 :UInt64;
  # Additive (#280): the joint retirement bound one artifact must fit. A staged artifact of
  # `chunks` chunks (the sum over its records of ceil(record bytes / 65536)) and `records`
  # records is refused RESOURCE_LIMIT exactly when
  # retireChunkCharge * chunks + retireRecordCharge * records > retireRowBudget.
  # 0 from an older daemon.
  retireRowBudget @13 :UInt64;
  retireChunkCharge @14 :UInt64;
  retireRecordCharge @15 :UInt64;
}

enum PressurePhase { ready @0; requested @1; awaitingReclamation @2; blocked @3; awaitingArchive @4; }
enum EvictionFrontier { exhausted @0; unitLimit @1; scanBudget @2; payloadBudget @3; catalogBudget @4; archiveLag @5; }
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
  # #211, additive at schema 71 (SCHEMA_VERSION is unchanged: an older reader ignores these
  # fields and an older server leaves them zero or absent). Times are Unix milliseconds, 0
  # when none; the session-local counters restart when the daemon does.
  observedAt @11 :UInt64;          # the clock of the last pass; every age is measured to it
  linkedBytes @12 :UInt64;         # linked store bytes the last pass measured
  episodeSince @13 :UInt64;        # intake has stayed refused since (0 while ready)
  phaseSince @14 :UInt64;          # the current non-ready phase began
  waitSince @15 :UInt64;           # the current wait for native reclamation began
  waitBeforeBytes @16 :UInt64;     # linked bytes when that wait began
  forcedCompactions @17 :UInt64;   # compactions forced because linked bytes did not fall
  waitsReleased @18 :UInt64;       # waits ended after a forced compaction left bytes unchanged
  stalledPasses @19 :UInt64;       # passes that retired nothing and moved nothing on
  refusedWrites @20 :UInt64;       # data writes answered STORAGE_PRESSURE by physical admission
  waitingOn @21 :OptionalText;     # why the last pass could do no more
  floorPausedSince @22 :UInt64;    # the free-space floor paused writes since (0 when not paused)
  floorPauses @23 :UInt64;         # free-space floor pauses begun
  floorResumes @24 :UInt64;        # free-space floor pauses ended
  floorRefusedWrites @25 :UInt64;  # writes refused below the free-space floor
  # #218, additive again (SCHEMA_VERSION unchanged; an older reader ignores these).
  forcedCompactionMs @26 :UInt64;  # wall time of the last forced compaction; 0 before one
  forcedCompactionFamilies @27 :List(CompactionFamily); # table and blob bytes per family around it
  forcedFailures @28 :UInt64;      # forced compactions that ended in a panic or native error
  nativeErrors @29 :UInt64;        # new RocksDB background errors the pressure passes saw
  # #267, additive (SCHEMA_VERSION unchanged; an older reader ignores these).
  floorRequests @30 :UInt64;       # free-space-floor episodes in which a refused write asked for reclaim
  floorRequestedBytes @31 :UInt64; # bytes below the floor at the last pass; 0 when none
  floorPasses @32 :UInt64;         # retirement passes run while the floor was the reason
  floorFreedBytes @33 :UInt64;     # linked bytes the current or last floor episode freed
  # #298, additive (SCHEMA_VERSION unchanged; an older server leaves these zero or false).
  diskBudget @34 :UInt64;          # the configured data budget
  maintenanceReserve @35 :UInt64;  # the configured reserve; intake is refused past budget - reserve
  archiveConfigured @36 :Bool;     # the store has an archive policy
  archiveHeld @37 :Bool;           # expiry and eviction are held for want of an archive
  reserveHeadroom @38 :Int64;      # budget - reserve - linkedBytes at the last pass; negative when over
}
struct CompactionFamily { family @0 :Text; beforeBytes @1 :UInt64; afterBytes @2 :UInt64; }

# One-time rewrite of table files written under an older options profile.
enum RecompressionPhase { running @0; paused @1; complete @2; }
struct RecompressionStatus {
  profile @0 :Text; phase @1 :RecompressionPhase; remainingBytes @2 :UInt64;
  waiting @3 :OptionalText; failure @4 :OptionalText;
}

enum WatchCoverage { reconciling @0; covered @1; }
struct ConsumeWatchStatus {
  id @0 :Text; root @1 :Text; coverage @2 :WatchCoverage;
  directories @3 :UInt32; candidates @4 :UInt32; deferred @5 :Bool;
  pendingClaims @6 :UInt32; completedClaims @7 :UInt64;
  reconciliations @8 :UInt64; coverageLosses @9 :UInt64;
  failure @10 :OptionalText;
  definition @11 :WatchDefinition; revision @12 :UInt64; enabled @13 :Bool;
  tailGenerationIds @14 :List(Text); tailReadBytes @15 :UInt64; tailDescriptors @16 :UInt32; tailCheckpointRecords @17 :UInt64; tailAdmissionWaiting @18 :Bool;
  # Schema 70: claims an administrator retired as unrecoverable; never counted as pending.
  retiredClaims @19 :List(RetiredClaim);
  # Additive (#280, ordinals 20 to 22): the watch's publish hold, how many of its pending claims
  # the hold holds, and the first-seen time of the oldest of them. A reader that does not know
  # these sees no hold.
  publishHold @20 :Bool; heldClaims @21 :UInt32; oldestHeldFirstSeen @22 :OptionalInstant;
}

# A claim retired unrecoverable by watchClaimRetire: claimed, its private source missing and
# never published. It leaves pendingClaims and is kept as this audit record.
struct RetiredClaim {
  claimId @0 :Text; relativePath @1 :Data; retiredAt @2 :Instant; caller @3 :Text; reason @4 :Text;
}

# Schema 71: one claim's state and durable publication history.
enum ClaimState { pending @0; complete @1; abandoned @2; }
struct ClaimRow {
  claimId @0 :Text; watch @1 :Text; state @2 :ClaimState; relativePath @3 :Data;
  # Absent for a claim reserved before schema 71.
  firstSeen @4 :OptionalInstant;
  lastAttempt @5 :OptionalInstant; attempts @6 :UInt32;
  lastPendingAt @7 :OptionalInstant; lastPendingReason @8 :OptionalText;
  # Completion or retirement time; absent while pending.
  finishedAt @9 :OptionalInstant;
}
# Window bounds are inclusive Unix milliseconds; an absent until means now.
struct WatchClaimsQuery { storeId @0 :Text; since @1 :Instant; until @2 :OptionalInstant; }
struct WatchClaims {
  storeId @0 :Text; asOf @1 :Instant; since @2 :Instant; until @3 :Instant;
  claims @4 :List(ClaimRow);
}

enum WatchMode { consume @0; tail @1; }
enum WatchReadiness { closeOrIdle @0; }
struct WatchAuthority {
  union { unacknowledged @0 :Void; finishedFilesDeleteAfterDurablePublish @1 :Void; activeFilesRetain @2 :Void; }
}
struct WatchDefinition {
  id @0 :Text; root @1 :Text; source @2 :Text; format @3 :Format;
  quietMs @4 :UInt32 = 30000; maxDirectories @5 :UInt32 = 1024;
  maxCandidates @6 :UInt32 = 4096; recursive @7 :Bool = true;
  mode @8 :WatchMode; readiness @9 :WatchReadiness; authority @10 :WatchAuthority;
  # Additive (#280, ordinal 11): holds publication of this finished-file watch's claims while
  # true; claims stay journaled and in flight. An older reader refuses a stored catalog that
  # carries it, by name. False is omitted from the stored form.
  publishHold @11 :Bool = false;
}
struct WatchInfo {
  storeId @0 :Text; definition @1 :WatchDefinition; revision @2 :UInt64; enabled @3 :Bool;
  retiredClaims @4 :List(RetiredClaim);
}
struct WatchList { storeId @0 :Text; watches @1 :List(WatchInfo); }
struct WatchPut {
  storeId @0 :Text; requestId @1 :Text; expectedRevision @2 :UInt64; definition @3 :WatchDefinition;
}
struct WatchRemove { storeId @0 :Text; requestId @1 :Text; id @2 :Text; expectedRevision @3 :UInt64; }
# Retire one unrecoverable claim of one watch; the daemon records the authenticated caller.
struct WatchClaimRetire { storeId @0 :Text; watch @1 :Text; claimId @2 :Text; reason @3 :Text; }
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
  tailGenerationId @6 :OptionalText;
}
enum HandoffPhase { reserved @0; claimed @1; published @2; removed @3; }
struct HandoffReceipt {
  request @0 :WatchHandoff; importId @1 :Text; phase @2 :HandoffPhase;
  artifactId @3 :OptionalText; durability @4 :Durability = durable;
  tailGenerationId @5 :OptionalText;
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
struct ReadBytes {
  union { evidence @0 :Evidence; fileGeneration @3 :TailGenerationQuery; }
  offset @1 :UInt64; length @2 :UInt32;
}
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
  requireCompleteIndex @8 :Bool;
  schemaVersion @9 :UInt32;
}
struct OptionalSourceIds { union { absent @0 :Void; value @1 :List(Text); } }
struct PinScope { storeId @0 :Text; id @1 :Text; expectedRevision @2 :OptionalRevision; }
struct QuerySelection { union { live @0 :Void; evidence @1 :List(Evidence); pin @2 :PinScope; } }
struct ValueQuery {
  scope @0 :Scope; path @1 :OptionalText; kind @2 :OptionalKind;
  lexical @3 :OptionalText; decoded @4 :OptionalText; cursor @5 :OptionalText;
}
enum Execution { complete @0; limited @1; }
enum Certainty { exact @0; incomplete @1; }
struct Coverage {
  visited @0 :UInt64; eligibleRecords @1 :Count; failedJsonRecords @2 :UInt64;
  nonJsonRecords @3 :UInt64; decodingFailures @4 :UInt64;
  knownEligibleRecords @5 :UInt64; diagnosticScope @6 :DiagnosticCoverage;
}
struct Count { union { unknown @0 :Void; known @1 :UInt64; } }
struct DiagnosticCoverage {
  evaluated @0 :UInt64; matched @1 :UInt64; excluded @2 :UInt64;
  unknown @3 :UInt64; membershipComplete @4 :Bool;
}
struct OptionalLimit { union { absent @0 :Void; value @1 :UInt64; } }
enum QueryProfileRole { jsonProjection @0; hostAdmission @1; textAnalyzer @2; textRanking @3; textCorpus @4; }
struct QueryProfile { role @0 :QueryProfileRole; id @1 :Text; }
struct QueryLimits {
  candidateVisits @0 :UInt64; jsonValueVisits @1 :OptionalLimit;
  fieldVisits @2 :OptionalLimit; postingVisits @3 :OptionalLimit;
  membershipMemberVisits @4 :UInt64; membershipRecordVisits @5 :UInt64;
  captureItems @6 :UInt64; captureBytes @7 :UInt64; rowBytes @8 :UInt64;
  metadataBytes @9 :UInt64; pageBytes @10 :UInt64; resultHandles @11 :UInt64;
  resultBytes @12 :UInt64; leaseMs @13 :UInt64;
  textMembershipBytes @14 :OptionalLimit; queryTextBytes @15 :OptionalLimit;
  queryTerms @16 :OptionalLimit; fieldConstraints @17 :OptionalLimit;
  compiledFieldBytes @18 :OptionalLimit; queryPathBytes @19 :OptionalLimit;
  keyAggregationBytes @20 :OptionalLimit;
}
enum ScopeSelectionKind { live @0; evidence @1; pin @2; }
struct CapturedPin {
  id @0 :Text; revision @1 :UInt64; expiresAtMs @2 :UInt64;
}
struct OptionalCapturedPin { union { absent @0 :Void; value @1 :CapturedPin; } }
struct QueryScopeReceipt {
  profile @0 :Text; id @1 :Text; selection @2 :ScopeSelectionKind;
  pin @3 :OptionalCapturedPin; directMembers @4 :Count; expandedRecords @5 :Count;
  hasExactSource @6 :Bool; sourceAlternatives @7 :UInt32; diagnosticDimensions @8 :UInt32;
  receivedFromMs @9 :UInt64; receivedBeforeMs @10 :UInt64;
  requireCompleteIndex @11 :Bool;
}
struct QueryCapture {
  page @0 :CatalogReadReceipt; profileIds @1 :List(QueryProfile);
  nativeGovernor @2 :OptionalText; limits @3 :QueryLimits;
  scope @4 :QueryScopeReceipt;
}
struct OptionalQueryCapture { union { absent @0 :Void; value @1 :QueryCapture; } }
struct Receipt {
  storeId @0 :Text; membershipAt @1 :Instant; execution @2 :Execution;
  certainty @3 :Certainty; coverage @4 :Coverage; capture @5 :OptionalQueryCapture;
}
struct Key { path @0 :Text; kind @1 :JsonKind; occurrences @2 :UInt64; records @3 :UInt64; }
struct Keys { receipt @0 :Receipt; rows @1 :List(Key); page @2 :CatalogReadReceipt; }
struct ValueMatch {
  recordId @0 :Text; artifactId @1 :Text; source @2 :Text; receivedAt @3 :Instant;
  sourceStart @4 :UInt64; sourceEnd @5 :UInt64; occurrence @6 :UInt64;
  path @7 :Text; kind @8 :JsonKind; lexical @9 :OptionalText; decoded @10 :OptionalText;
}
struct Values { receipt @0 :Receipt; rows @1 :List(ValueMatch); page @2 :CatalogReadReceipt; }
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
struct TextQuery { scope @0 :Scope; text @1 :Text; cursor @2 :OptionalText; }
struct TextSearchPage { page @0 :CatalogReadReceipt; result @1 :TextResults; }
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
struct ContextLimits {
  directionVisits @0 :UInt64; catalogBytes @1 :UInt64; responseBytes @2 :UInt64;
  outputRecords @3 :UInt32; requestedBefore @4 :UInt32; requestedAfter @5 :UInt32;
  membershipMemberVisits @6 :UInt64; membershipRecordVisits @7 :UInt64;
  scopeReceiptBytes @8 :UInt64;
}
struct ContextCapture {
  snapshotId @0 :Text; asOf @1 :Instant; hostProfile @2 :Text;
  limits @3 :ContextLimits; nativeGovernor @4 :OptionalText;
  scope @5 :QueryScopeReceipt;
}
struct Context {
  receipt @0 :Receipt; anchor @1 :Record; before @2 :ContextSide; after @3 :ContextSide;
  ordering @4 :ContextOrder; continuityUnknown @5 :Bool = true; capture @6 :ContextCapture;
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
struct CatalogReadReceipt {
  storeId @0 :Text; snapshotId @1 :Text; asOf @2 :Instant; leaseExpiresAt @3 :Instant;
  totalItems @4 :UInt64; pageStart @5 :UInt64; nextCursor @6 :OptionalText;
  pageSize @7 :UInt32; scanBudget @8 :UInt32; scanned @9 :UInt64;
  # #158: set on a listing page that answers before its build has read what an unfinished
  # online migration owes. The page has no rows and nextCursor resumes the same build.
  build @10 :CatalogBuild;
}
# How far a listing build has read: catalog entries read of the entries the migration still
# owed at the build's snapshot. totalPending while the store counters seed (total is 0).
struct CatalogBuild { read @0 :UInt64; total @1 :UInt64; totalPending @2 :Bool; }
struct PinMembers { pin @0 :PinSummary; members @1 :List(Evidence); }
struct PinReadPage {
  receipt @0 :CatalogReadReceipt;
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
  cursor @5 :OptionalText;
  groupByArtifact @6 :Bool;
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
  fieldVisits @3 :UInt64; qualifiedRecords @4 :Count;
  timeBasis @5 :TimeBasis; eventCoverage @6 :EventCoverage;
  knownQualifiedRecords @7 :UInt64;
  page @8 :CatalogReadReceipt;
  artifactGroups @9 :OptionalArtifactMatchGroups;
  # Schema 69: qualified records placed on the event timeline by receipt time; zero for
  # a receipt-time read.
  eventTimeFallback @10 :UInt64;
}
struct ArtifactMatchGroup { artifactId @0 :Text; capturedMatches @1 :UInt64; pageIndices @2 :List(UInt32); }
struct OptionalArtifactMatchGroups { union { absent @0 :Void; value @1 :List(ArtifactMatchGroup); } }
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

# One read-only membership instant; acceptance never implies source removal.
struct IngestSelector { union { request @0 :Text; import @1 :Text; handoff @2 :Text; } }
struct IngestStatusQuery {
  storeId @0 :Text; schemaVersion @1 :UInt32; selector @2 :IngestSelector;
  scanBudget @3 :UInt32 = 10000; catalogBytes @4 :UInt32 = 33554432;
}
struct IngestIndexCounts {
  complete @0 :UInt64; failed @1 :UInt64; incomplete @2 :UInt64;
  notApplicable @3 :UInt64; unavailable @4 :UInt64; pending @5 :UInt64;
}
enum IngestDeletion { notManaged @0; reserved @1; claimed @2; published @3; removed @4; retired @5; }
struct IngestAcceptance {
  artifactId @0 :Text; source @1 :Text; format @2 :Format;
  receivedAt @3 :Instant; expiresAt @4 :Instant; byteLength @5 :UInt64;
  payloadId @6 :Text; recordCount @7 :UInt64;
  durability @8 :Durability = durable; bufferedThrough @9 :OptionalDurabilityWatermark;
}
struct IngestOwnership {
  watch @0 :Text; relativePath @1 :Data; source @2 :Text; format @3 :Format;
  observed @4 :FileGeneration; claimed @5 :OptionalFileGeneration;
}
struct OptionalFileGeneration { union { absent @0 :Void; value @1 :FileGeneration; } }
struct OptionalIngestOwnership { union { absent @0 :Void; value @1 :IngestOwnership; } }
struct OptionalIngestAcceptance { union { absent @0 :Void; value @1 :IngestAcceptance; } }
struct OptionalImportProgress { union { absent @0 :Void; value @1 :ImportProgress; } }
struct OptionalImportPreparation { union { absent @0 :Void; value @1 :ImportPreparation; } }
struct OptionalTailGeneration { union { absent @0 :Void; value @1 :TailGeneration; } }
struct OptionalRecordCount { union { unknown @0 :Void; known @1 :UInt64; } }
struct IngestStatus {
  storeId @0 :Text; selector @1 :IngestSelector; asOf @2 :Instant;
  requestId @3 :Text; importId @4 :OptionalText;
  staging @5 :OptionalImportProgress; preparation @6 :OptionalImportPreparation;
  accepted @7 :OptionalIngestAcceptance; tail @8 :OptionalTailGeneration;
  deletion @9 :IngestDeletion; removedAt @10 :OptionalInstant;
  ownership @11 :OptionalIngestOwnership; totalRecords @12 :OptionalRecordCount;
  inspectedRecords @13 :UInt64; json @14 :IngestIndexCounts;
  rdf @15 :IngestIndexCounts; text @16 :IngestIndexCounts;
}

# Schema 71 additive (#285): one set part's receipt state by request id. Read-only; carries no
# record counts. A daemon without this method answers the protocol's unimplemented error.
struct IngestPartReceiptQuery { storeId @0 :Text; schemaVersion @1 :UInt32; requestId @2 :Text; }
struct IngestPartReceipt { requestId @0 :Text; artifactId @1 :Text; byteLength @2 :UInt64; format @3 :Format; }
struct IngestPartState {
  union {
    absent @0 :Void;                    # no kept receipt: the part is not published
    stored @1 :IngestPartReceipt;       # the receipt's artifact is stored and not evicted
    evicted @2 :IngestPartReceipt;      # the artifact is gone and no record of it is stored
    evicting @3 :IngestPartReceipt;     # eviction began; some record of it is still stored
  }
}

# Schema 71 additive (#323): the artifacts that hold one payload digest, read from the payload id
# index. `complete` is false while the `payload-index` migration runs: every id listed is held,
# and an absent one is unknown, never absent.
struct PayloadLookupQuery {
  storeId @0 :Text; schemaVersion @1 :UInt32; payloadId @2 :Text;
  cursor @3 :OptionalText; pageSize @4 :UInt32 = 200;
}
struct PayloadLookupPage {
  storeId @0 :Text; payloadId @1 :Text; artifactIds @2 :List(Text);
  next @3 :OptionalText; complete @4 :Bool;
}

# Complete logical raw-payload account; physical files and indexes are excluded.
struct RetentionMetricsQuery {
  storeId @0 :Text; schemaVersion @1 :UInt32;
  maxVisits @2 :UInt32 = 10000; catalogBytes @3 :UInt32 = 33554432;
  payloadBytes @4 :UInt32 = 67108864;
}
struct RetentionMetrics {
  storeId @0 :Text; asOf @1 :Instant;
  liveRecords @2 :UInt64; liveArtifacts @3 :UInt64;
  pinnedRecords @4 :UInt64; pinnedArtifacts @5 :UInt64;
  oldestReceivedAt @6 :OptionalInstant; retainedWindowMs @7 :UInt64;
  ownedPayloadBytes @8 :UInt64; livePayloadBytes @9 :UInt64;
  pinHeldPayloadBytes @10 :UInt64; retirablePayloadBytes @11 :UInt64;
  unclassifiedPayloadBytes @12 :UInt64; payloadChunks @13 :UInt64;
  visits @14 :UInt64;
}

struct ResultNextQuery { storeId @0 :Text; schemaVersion @1 :UInt32; cursor @2 :Text; }
struct ResultReleaseQuery { storeId @0 :Text; schemaVersion @1 :UInt32; snapshotId @2 :Text; }
struct ResultReleased { storeId @0 :Text; snapshotId @1 :Text; released @2 :Bool; }
struct ResultPage {
  union {
    records @0 :RecordsResults;
    text @1 :TextSearchPage;
    keys @2 :Keys;
    values @3 :Values;
    artifacts @4 :ArtifactListPage;
    sources @5 :SourceListPage;
    pins @6 :PinReadPage;
    sparql @7 :SparqlPage;
  }
}
interface Sparrow {
  status @0 () -> (result :Result(Status));
  ingest @1 (request :Ingest) -> (result :Result(Artifact));
  artifactGet @2 (artifact :ArtifactRef) -> (result :Result(Artifact));
  recordGet @3 (record :RecordRef) -> (result :Result(Record));
  bytesRead @4 (request :ReadBytes) -> (result :Result(Bytes));
  pin @5 (request :PinRequest) -> (result :Result(Pin));
  pinRelease @6 (request :Release) -> (result :Result(Released));
  keysList @7 (scope :Scope, cursor :OptionalText, path :OptionalText) -> (result :Result(Keys));
  valuesSearch @8 (query :ValueQuery) -> (result :Result(Values));
  textSearch @9 (query :TextQuery) -> (result :Result(TextSearchPage));
  recordsContext @10 (query :ContextQuery) -> (result :Result(Context));
  rdfInspect @11 (record :RecordRef) -> (result :Result(RdfInspection));
  pinRenew @12 (request :Renew) -> (result :Result(Pin));
  pinRead @13 (query :PinReadQuery) -> (result :Result(PinReadPage));
  recordsSearch @14 (query :RecordQuery) -> (result :Result(RecordsResults));
  sourceTimeDefine @15 (definition :SourceTimeDefinition) -> (result :Result(SourceTimeProfile));
  sourceTimeGet @16 (source :Text, schemaVersion :UInt32) -> (result :Result(OptionalSourceTimeProfile));
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
  discover @34 () -> (result :Result(Discovery));
  artifactsList @35 (query :ArtifactListQuery) -> (result :Result(ArtifactListPage));
  sourcesList @36 (query :SourceListQuery) -> (result :Result(SourceListPage));
  resultNext @37 (query :ResultNextQuery) -> (result :Result(ResultPage));
  producerStreamGet @38 (query :ProducerStreamQuery) -> (result :Result(ProducerStream));
  tailGenerationGet @39 (query :TailGenerationQuery) -> (result :Result(TailGeneration));
  tailFinish @40 (request :TailFinishIntent) -> (result :Result(TailGeneration));
  tailFinishGet @41 (request :TailFinishIntent) -> (result :Result(TailGeneration));
  resultRelease @42 (query :ResultReleaseQuery) -> (result :Result(ResultReleased));
  syslogListenerGet @43 (query :SyslogListenerQuery) -> (result :Result(SyslogListenerStatus));
  receiverWindowGet @44 (query :ReceiverWindowQuery) -> (result :Result(ReceiverWindow));
  ingestStatus @45 (query :IngestStatusQuery) -> (result :Result(IngestStatus));
  retentionMetrics @46 (query :RetentionMetricsQuery) -> (result :Result(RetentionMetrics));
  sparqlQuery @47 (query :SparqlQuery) -> (result :Result(SparqlPage));
  correlate @48 (query :CorrelateQuery) -> (result :Result(SparqlPage));
  sparqlExplain @49 (query :SparqlQuery) -> (result :Result(SparqlPage));
  # Public TCP only: a fresh single-use 32-byte challenge for shared-secret CHAP.
  authChallenge @50 () -> (result :Result(TcpChallenge));
  # Public TCP only: consume the challenge; success grants an ungated session capability.
  authenticate @51 (response :Data) -> (result :Result(TcpSession));
  # Administration: retire a claim that can never complete (claimed, private source missing,
  # never published). Refused for a recoverable or published claim.
  watchClaimRetire @52 (request :WatchClaimRetire) -> (result :Result(RetiredClaim));
  # Removed (#330). The ordinal stays; the server answers UNSUPPORTED (a query-only daemon: READ_ONLY).
  backup @53 (storeId :Text) -> (result :Result(BackupStatus));
  # Schema 71: read-only per-claim states; every pending claim and every completed or
  # abandoned claim whose pending span (reservation to latest pending attempt) overlaps the
  # window; an abandoned claim is also listed when retired inside it.
  watchClaims @54 (query :WatchClaimsQuery) -> (result :Result(WatchClaims));
  # Schema 71 additive (#285): one set part's receipt state by request id, read-only. A daemon
  # without this method answers the protocol's unimplemented error.
  ingestPartReceipt @55 (query :IngestPartReceiptQuery) -> (result :Result(IngestPartState));
  # Schema 71 additive (#323): the held artifacts of one payload digest, read-only. A daemon
  # without this method answers the protocol's unimplemented error.
  payloadLookup @56 (query :PayloadLookupQuery) -> (result :Result(PayloadLookupPage));
}

struct TcpChallenge { nonce @0 :Data; }
struct TcpSession { session @0 :Sparrow; }

# Page-local, child-before-parent term nodes. IDs never identify persisted terms.
enum RdfDirection { absent @0; ltr @1; rtl @2; }
struct RdfBlank { label @0 :Text; scope @1 :UInt32; }
struct RdfLiteral { lexical @0 :Text; datatype @1 :Text; language @2 :OptionalText; direction @3 :RdfDirection; }
struct RdfTripleTerm { subject @0 :UInt32; predicate @1 :UInt32; object @2 :UInt32; }
struct RdfTermNode { union { iri @0 :Text; blank @1 :RdfBlank; literal @2 :RdfLiteral; triple @3 :RdfTripleTerm; } }
struct OptionalRdfTerm { union { unbound @0 :Void; value @1 :UInt32; } }
struct SparqlBinding { name @0 :Text; term @1 :UInt32; }
struct RdfLayers { metadata @0 :Bool = true; diagnostic @1 :Bool = true; content @2 :Bool = true; }
struct SparqlLimits {
  fuel @0 :UInt64 = 1000000; answerRows @1 :UInt64 = 10000;
  intermediateCells @2 :UInt64 = 1000000; scratchBytes @3 :UInt64 = 33554432;
  liveBytes @4 :UInt64 = 134217728; requests @5 :UInt64 = 100000;
  ioBytes @6 :UInt64 = 134217728; rowVisits @7 :UInt64 = 100000;
  sourceLeaseMs @8 :UInt64 = 30000;
}
struct SparqlQuery {
  storeId @0 :Text; schemaVersion @1 :UInt32; scope @2 :Scope;
  query @3 :Text; baseIri @4 :OptionalText; layers @5 :RdfLayers;
  terms @6 :List(RdfTermNode); bindings @7 :List(SparqlBinding); limits @8 :SparqlLimits;
  textRelations @9 :OptionalStoredTextRelations;
}
struct StoredTextRelations { rankedIri @0 :Text; spansIri @1 :Text; }
struct OptionalStoredTextRelations { union { absent @0 :Void; value @1 :StoredTextRelations; } }
struct StoredTextCoverage { ready @0 :UInt64; failedOrIncomplete @1 :UInt64; notApplicable @2 :UInt64; }
struct StoredTextCapture {
  relations @0 :StoredTextRelations; corpusProfile @1 :Text; rankingProfile @2 :Text;
  analyzer @3 :Text; corpusId @4 :Text; documents @5 :UInt64; tokens @6 :WideInteger;
  coverage @7 :StoredTextCoverage; membershipComplete @8 :Bool;
  profile @9 :Text;
}
struct OptionalStoredTextCapture { union { absent @0 :Void; value @1 :StoredTextCapture; } }
enum GovernorDimension { fuel @0; answerRows @1; intermediateCells @2; scratchBytes @3; remoteRequests @4; udfDepth @5; pages @6; bytes @7; }
struct GovernorUse { dimension @0 :GovernorDimension; consumed @1 :UInt64; ceiling @2 :UInt64; }
struct GovernorBudget { dimension @0 :GovernorDimension; limit @1 :UInt64; consumed @2 :UInt64; }
struct GovernorRefusal { dimension @0 :GovernorDimension; limit @1 :UInt64; estimate @2 :UInt64; }
enum StopCause { cancelled @0; deadline @1; }
struct GovernorStop { union { absent @0 :Void; budget @1 :GovernorBudget; refused @2 :GovernorRefusal; stopped @3 :StopCause; } }
enum SilencedTargetKind { service @0; load @1; }
enum SilencedFailureKind { transport @0; decode @1; disabled @2; unconfigured @3; denied @4; hostDenied @5; notAnIri @6; fault @7; }
struct SilencedFailure { targetKind @0 :SilencedTargetKind; target @1 :Text; kind @2 :SilencedFailureKind; message @3 :Text; }
struct ExpressionErrorCount { code @0 :Text; count @1 :UInt64; }
struct NativeGovernorReceipt { profile @0 :Text; resources @1 :List(GovernorUse); stop @2 :GovernorStop; silenced @3 :List(SilencedFailure); expressionErrors @4 :List(ExpressionErrorCount); }
struct RdfReadEvidence { requests @0 :UInt64; admittedBytes @1 :UInt64; rowVisits @2 :UInt64; liveBytes @3 :UInt64; peakLiveBytes @4 :UInt64; }
struct SparqlCoverage {
  visited @0 :UInt64; selectedRecords @1 :Count; knownSelectedRecords @2 :UInt64;
  contentReady @3 :UInt64; contentFailed @4 :UInt64; contentNotApplicable @5 :UInt64;
  diagnostics @6 :DiagnosticCoverage; catalogEntries @7 :UInt64; complete @8 :Bool;
  artifactsVisited @9 :UInt64; knownSelectedArtifacts @10 :UInt64;
  artifactDiagnosticUnknown @11 :UInt64;
  eventTimeKnown @12 :UInt64;
  diagnosticKnown @13 :List(DiagnosticKnownCount);
  # Selected record graphs whose receipt-time fallback facts the reader derives (not stored).
  eventFallbackPending @14 :UInt64;
}
struct DiagnosticKnownCount { dimension @0 :DiagnosticDimension; known @1 :UInt64; }
enum CorrelationClock { received @0; event @1; }
struct CorrelationInterval { clock @0 :CorrelationClock; from @1 :Instant; before @2 :Instant; }
struct CorrelationDiagnostic { dimension @0 :DiagnosticDimension; value @1 :Text; }
struct CorrelationSelector {
  union { evidence @0 :List(Evidence); diagnostic @1 :CorrelationDiagnostic; source @2 :Text; time @3 :CorrelationInterval; }
}
struct CorrelationRelation { union { traceId @0 :Void; spanId @1 :Void; source @2 :Void; time @3 :CorrelationInterval; } }
struct CorrelateQuery { storeId @0 :Text; scope @1 :Scope; selector @2 :CorrelationSelector; relation @3 :CorrelationRelation; limits @4 :SparqlLimits; schemaVersion @5 :UInt32; }
struct CorrelationCapture { profile @0 :Text; queryId @1 :Text; selector @2 :CorrelationSelector; relation @3 :CorrelationRelation; }
struct OptionalCorrelationCapture { union { absent @0 :Void; value @1 :CorrelationCapture; } }
struct SparqlHostLimits {
  source @0 :SparqlLimits; pageBytes @1 :UInt64; metadataBytes @2 :UInt64;
  resultHandles @3 :UInt64; resultBytes @4 :UInt64; resultLeaseMs @5 :UInt64;
  scopeCatalogBytes @6 :UInt64; catalogExpansionFactor @7 :UInt64;
  queryTextBytes @8 :UInt64; bindingBytes @9 :UInt64; bindingNodes @10 :UInt32;
  sourceProfile @11 :Text;
  scopeReceiptBytes @12 :UInt64;
}
enum SparqlForm { select @0; ask @1; graph @2; }
struct SparqlSolution { cells @0 :List(OptionalRdfTerm); }
struct RdfQuad { subject @0 :UInt32; predicate @1 :UInt32; object @2 :UInt32; graph @3 :OptionalRdfTerm; }
struct RdfReifier { reifier @0 :UInt32; triple @1 :UInt32; graph @2 :OptionalRdfTerm; }
struct RdfAnnotation { reifier @0 :UInt32; predicate @1 :UInt32; object @2 :UInt32; graph @3 :OptionalRdfTerm; }
struct SparqlGraphItem { auxiliary @0 :Bool; union { declaration @1 :UInt32; quad @2 :RdfQuad; reifier @3 :RdfReifier; annotation @4 :RdfAnnotation; } }
struct SparqlItem { union { solution @0 :SparqlSolution; boolean @1 :Bool; graph @2 :SparqlGraphItem; } }
struct SparqlData { form @0 :SparqlForm; variables @1 :List(Text); terms @2 :List(RdfTermNode); items @3 :List(SparqlItem); }
struct SparqlPartial { data @0 :SparqlData; positionalPrefix @1 :Bool; }
struct SparqlOutcome { union { complete @0 :SparqlData; certain @1 :SparqlPartial; atMost @2 :SparqlPartial; unknown @3 :Text; failure @4 :Failure; explanation @5 :SparqlExplanation; } }
enum NativeReadOperation { query @0; explain @1; }
struct NativeChargeRate { label @0 :Text; cost @1 :UInt64; }
struct NativePlanEstimate { rows @0 :UInt64; peakRows @1 :UInt64; columns @2 :UInt64; peakCells @3 :UInt64; }
struct OptionalNativePlanEstimate { union { absent @0 :Void; value @1 :NativePlanEstimate; } }
struct NativeNodeCharges {
  ordinal @0 :UInt64; depth @1 :UInt64; label @2 :Text;
  fuel @3 :List(UInt64); fuelTotal @4 :UInt64; rows @5 :UInt64; cells @6 :UInt64;
  estimate @7 :OptionalNativePlanEstimate;
}
enum NativeReadResidency { absent @0; resident @1; nonResident @2; mixed @3; }
struct NativeAccessEstimate {
  work @0 :UInt64; rows @1 :UInt64; pages @2 :OptionalLimit;
  residency @3 :NativeReadResidency; cardinalityFallback @4 :OptionalLimit;
}
struct OptionalNativeAccessEstimate { union { absent @0 :Void; value @1 :NativeAccessEstimate; } }
struct NativeShapeReport {
  node @0 :OptionalLimit; operator @1 :OptionalText; provider @2 :OptionalText;
  registry @3 :OptionalLimit; sourceVisits @4 :UInt64; transitions @5 :UInt64;
  outputRows @6 :UInt64; estimate @7 :OptionalNativeAccessEstimate;
}
enum NativeRelationVolatility { stable @0; volatile @1; }
struct NativeRelationMode { code @0 :Text; rowsPerInvocation @1 :UInt64; }
struct NativeRelationDescriptor {
  iri @0 :Text; subjectArity @1 :UInt32; objectArity @2 :UInt32;
  volatility @3 :NativeRelationVolatility; modes @4 :List(NativeRelationMode);
}
struct NativeExplanationItem { union { node @0 :NativeNodeCharges; joinOrder @1 :Text; statistic @2 :Text; shape @3 :NativeShapeReport; relation @4 :NativeRelationDescriptor; } }
struct NativeStatementIndexes { reifierTargets @0 :Bool; reifierComponents @1 :Bool; annotationValues @2 :Bool; }
struct SparqlExplanation {
  profile @0 :Text; version @1 :UInt32; digest @2 :Text; stopPollFuel @3 :UInt64;
  schedule @4 :List(NativeChargeRate); indexes @5 :NativeStatementIndexes;
  items @6 :List(NativeExplanationItem);
  # Registered ordinary property functions are described in paged relation items.
  # Native ranked declarations and custom aggregates are not injected here.
  relations @7 :List(Text); aggregates @8 :List(Text);
}
struct SparqlPage {
  page @0 :CatalogReadReceipt; sequence @1 :UInt64; layers @2 :RdfLayers;
  coverage @3 :SparqlCoverage; governors @4 :NativeGovernorReceipt;
  evidence @5 :RdfReadEvidence; limits @6 :SparqlHostLimits; outcome @7 :SparqlOutcome;
  diagnostic @8 :OptionalNativeDiagnostic;
  scope @9 :QueryScopeReceipt;
  correlation @10 :OptionalCorrelationCapture;
  operation @11 :NativeReadOperation;
  text @12 :OptionalStoredTextCapture;
}
enum NativeSeverity { error @0; warning @1; note @2; info @3; }
enum NativeStorageFailure { absent @0; capacity @1; sizeOverflow @2; allocationFailed @3; admissionFailed @4; formattingFailed @5; }
struct NativeCapacity { space @0 :Text; attempted @1 :WideInteger; limit @2 :WideInteger; }
struct NativeLocation {
  path @0 :OptionalText; logical @1 :OptionalText; subject @2 :OptionalText;
  line @3 :OptionalLimit; column @4 :OptionalLimit; gtsTermId @5 :OptionalLimit;
  gtsQuadIndex @6 :OptionalLimit; gtsReifierId @7 :OptionalLimit;
  gtsFrameIndex @8 :OptionalLimit; gtsSegmentIndex @9 :OptionalLimit;
}
struct NativeParameter { name @0 :Text; union { text @1 :Text; unsigned @2 :UInt64; signed @3 :Int64; boolean @4 :Bool; character @5 :UInt32; } }
# Outer presentation first, followed by each nested detail in order.
struct NativePresentation { messageId @0 :Text; english @1 :Text; parameters @2 :List(NativeParameter); }
struct NativeDiagnostic {
  severity @0 :NativeSeverity; code @1 :Text; message @2 :Text; detail @3 :OptionalText;
  location @4 :NativeLocation; hasLocation @5 :Bool;
  presentation @6 :List(NativePresentation); storageFailure @7 :NativeStorageFailure;
  capacity @8 :NativeCapacity;
}
struct OptionalNativeDiagnostic { union { absent @0 :Void; value @1 :NativeDiagnostic; } }
