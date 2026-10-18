# Development runtime

The installed server is the enabled persistent user unit `sparrow.service`,
configured for typed Cap'n Proto at `0.0.0.0:518` and the existing private Unix
socket. It is running and both typed endpoints were verified on 2026-10-18 UTC
after the approved checkpoint recovery and schema-57 adoption below. It uses
schema 57 and store format 70. The stable installed
client is `~/.local/bin/sparrow`. The latest installed daemon source is
`974160f`; see [schema-57 live adoption](#schema-57-live-adoption) for
its adoption evidence and [persistent service adoption](#persistent-service-adoption)
for unit configuration. Earlier
transient-unit and schema sections below retain historical evidence.

## Schema-57 live adoption

Signed source `974160f2d397977f10b6e06e1898a153793203c4` includes the shared
scope receipts from `5c40cc8` and graph/context source qualification below. The
explicit default daemon/client build emitted `core_features=[]`; both selected
artifacts were copied to exclusive versioned paths and hash-verified before any
alias change:

- `~/.local/libexec/sparrow/sparrowd-schema57-974160f`, SHA-256
  `33e499370c14866b63bb4b919572d5370e3290320e528480c5bd97a50415de73`.
- `~/.local/libexec/sparrow/sparrow-schema57-974160f`, SHA-256
  `c1b4e2ae911884b2a35c8ad56501b40e4b9e884318e3168c93c182d366dc442f`.

The private default-image fixture passed CLI/MCP record, text, key, value,
context and native graph scope parity, actual pin revision/deadline capture,
renewal/release and retained graph replay. It ended with three records, no pins
and no retained query handles, then shut down cleanly. A fresh watch-free store
correctly uses authority-family format 68; the first harness wrongly expected
the live store's format 70. A startup probe established the difference, and the
corrected fixture passed without a product change. Failed trial/probe artifacts
remain under `schema57-scope-*` for review.

Configuration and unit hashes matched their previous adoption receipts. The
qualified daemon received only `cap_net_bind_service=ep`. The old service stopped
cleanly before an exclusively locked, hash-verified 111-file closed backup was
made at `schema57-pre-adoption-closed-backup-20261018`. Each stable alias then
switched atomically to its qualified image. The enabled persistent unit started
at 2026-10-18 05:31:21 MDT (11:31:21 UTC), PID 858590, on the unchanged TCP and
Unix endpoints. No checkpoint recovery was repeated.

Live TCP and Unix agree on schema 57, format 70, the original
`store:46c88e949ccec658600b66c7becd5bb7`, 217 retained records and healthy watches.
Both original recovered generation receipts are exactly equal to their schema-55
receipts, including declarations, clocks, gaps and offsets 37/0. All primary query
surfaces share the same frozen scope for a retained selected record. SELECT,
ASK and CONSTRUCT each passed in one page; DESCRIBE passed through 16 pages over
each endpoint, with exact native certainty and unchanged scope on continuation.
The schema-55 client refuses discovery against schema 57. Live MCP native ASK,
context and record results carry the same scope as both CLI endpoints.

All 253 current artifact/record targets were fetched over typed TCP in ranges of
at most 1 MiB. Their full BLAKE3 identities match the schema-55 complete-copy
baseline: 36,005,590 verified bytes, including overlapping artifact and record
ranges. All retained query handles returned to zero. The closed backup remains
unopened; this adoption does not establish production-volume, physical-reclamation,
quota or host-reboot acceptance.

Evidence under `.stage/native-graph-integration/` includes `schema57-runtime-*`,
`schema57-scope-fixture-qualified.json`, `schema57-adoption-prerequisites.json`,
`schema57-pre-adoption-closed-backup.json`, `schema57-readiness-attempts.json`,
`schema57-live-scope-qualified.json`, `schema57-live-mcp-qualified.json`,
`schema57-live-bytes-qualified.json` and `schema57-final-readback.json`.

## Schema-57 graph/context scope source qualification

Schema 57 extends the existing scope receipt owner to native graph and context
queries. Pin resolution, normalized identity and typed encoding/decoding stay
shared with record/text/key/value captures. Graph results retain scope workspace
through the existing owned native reservation, including native query failures;
continuations preserve the original scope after pin renewal and release. Context
reserves receipt workspace and response bytes within its existing aggregate caps.
Admission profiles advance to `sparrow-rdf-source-v4` and
`sparrow-context-capture/3`, with an explicit per-copy 64 KiB scope allowance.
Persistent store format remains 70.

The full release gate passed 27 core unit, 185 storage and 64 RPC tests plus all
other workspace checks, seeded census self-tests and the pinned cross-workspace
gate: 281 ownership jobs, no distinct exceptions. The new injected-time regression
compares record, graph and context scope receipts, renews/releases the pin, expires
the original evidence and replays a native graph continuation unchanged. It also
checks a native query failure and selected recordless artifact. TCP CLI/Unix MCP
tests retain identical scope receipts across all native read forms and pages;
selected and direction-limited context replies preserve their requested scope.
Evidence is `.stage/native-graph-integration/graph-context-scope-full-check.log`;
the ordinary signed-commit hook records its gate in `graph-context-scope-commit.log`.

The default-image qualification and adoption above are separate from these source
gates. Scope receipts do not finish native text relations, governed EXPLAIN,
correlation, bounded physical reclamation or production acceptance.

## Schema-56 scope receipt source qualification

Schema 56 adds frozen logical scope receipts to the shared paged record/text/key/value
capture, including artifact groups. Selection resolution records the actual pin
revision/deadline once; anonymous staging retains that receipt through renewal,
release and logical evidence retirement. Normalized source/diagnostic alternatives
and evidence ID sets use native PurRDF streaming framing and BLAKE3. The compact
receipt does not echo large selector values or replace snapshot/corpus identity.
See [the contract](interfaces.md#frozen-scope-receipts-schema-56).

Release qualification passed 27 core unit, 184 storage and 64 RPC tests, including
CLI/MCP pin-page replay and key discovery parity, malformed scope rejection,
normalized identities, and a 100,000-byte exact-source selector with all six
128-alternative dimensions. That selector's scope receipt stays below 1 KiB.
Formatting, release Clippy with warnings denied, all other workspace tests,
seeded census self-tests and the pinned cross-workspace gate passed: 281 ledger
jobs and no distinct exceptions. Evidence is
`.stage/native-graph-integration/scope-full-check.log`; the ordinary signed-commit
hook records its independent gate in `scope-commit.log`.

Schema 56 was not installed as a separate version; schema 57 includes this work.
The earlier TCP/Unix readback in `scope-pre-runtime-readback.json`
confirmed schema 55, format 70, 217 records, healthy watches and exact equality
of both original recovered generation receipts. No recovery was repeated.

## Schema-55 live adoption

Signed source `f715df8e1a520ebd897650e21bad80352948e1f8` contains the
Source/Stream implementation from `01626f1` and its full-gate qualification below.
Native query qualification on the migrated private store passed SELECT, ASK,
CONSTRUCT and DESCRIBE plus independent Record/Artifact counts of 217/36.
Two typed source-catalog pages agreed exactly with 22 retained RDF Sources,
including their published IRIs and exact labels. Typed record positions agreed
with 7 file-generation Streams, 4 producer Streams, and 22 positioned records
across 5 source/kind groups. Native query reports were identical after reopening;
all 112 original qualification-store file hashes remained unchanged.
The first harness incorrectly required a single source page; the corrected
probe consumes every page and releases its original snapshot. Its source is
archived as `schema55_copy_probe.rs`; the temporary example was removed before
ordinary runtime packaging.
Evidence under `.stage/native-graph-integration/` includes
`schema55-query-build-v2.{jsonl,log}`, `schema55-query-image-v2.json`,
`schema55-query-v2-{first,reopen}.{json,log}` and `schema55-query-qualified.json`.

Ordinary runtime packaging was explicitly selected with
`cargo build --locked --release -p sparrow-server --bins --message-format=json`.
Its emitted core feature list is empty. The selected installed images are:

- `~/.local/libexec/sparrow/sparrowd-schema55-f715df8`, SHA-256
  `4be1ae5eef417ffa85ca05c50d2e946b9e7dc54b95815799b1bebf46fec81530`.
- `~/.local/libexec/sparrow/sparrow-schema55-f715df8`, SHA-256
  `0c58775168ef445ce2d778545670b91a719c3751875c6c3b6cf1642c19966443`.

A fresh private daemon using that exact pair passed the existing diagnostic,
artifact-length, strict-refusal and permissive-coverage agent scenarios. Added
checks verified Source IRI agreement with typed source discovery, default-union
deduplication, named occurrence contexts, metadata-layer exclusion, record-only
Source provenance without whole-parent membership, and no invented Stream for an
ordinary import. CLI/MCP Source results agreed. The fixture shut down cleanly with
zero graph handles/bytes and no watches. Its receipts use `schema55-agent-*`.

Image hashes and the daemon's sole `cap_net_bind_service=ep` capability were
verified before SIGINT drain. With the unit inactive, PID zero and Unix socket
absent, an exclusive native lock protected a synchronized 109-file closed backup
at `schema55-pre-adoption-closed-backup-20261018`. Every regular-file hash matched
the closed source before and after copying. Both aliases were then switched and
the enabled user unit started at 2026-10-18 09:42:47 UTC, observed PID 2584986.
The first TCP readiness probe succeeded. Verification binds the installed image
hashes and capability to the configured aliases, unit start and typed behavior.
Configuration, unit and both watch declarations retained their
recorded identities. No checkpoint recovery was repeated.

Live TCP binds `0.0.0.0:518`; TCP and Unix agree on schema 55, format 70, the
original store identity, 217 records, 213 payload chunks and no watch failures.
All four native read forms, six-page DESCRIBE, cross-endpoint continuation/release
and CLI/MCP agreement passed. Source discovery consumed both catalog pages and
released its snapshot across endpoints. Native Source, Stream and position-join
results agreed over TCP, Unix and MCP: 22 Sources, 11 Streams and 22 positioned
records. Populated-artifact metadata and layer exclusion still match the original
catalog. Every recovered generation receipt is exactly equal to its pre-adoption
receipt over both endpoints, preserving declarations, clocks, gaps and offsets.
Acceptance ingested no live input. All read handles/bytes are zero and the fresh
closed backup's complete hash map remains unchanged.

Source selection admits 12,194,428 bytes, the Stream-kind aggregate 5,572,146,
and the record/Stream/Source join 19,430,348 at unchanged ceilings and source
profile v3. Accounted peak live bytes remain 115,740,672. These finite logical
accounts do not measure RSS or physical I/O. Adoption evidence includes
`schema55-runtime-*`, `schema55-adoption-prerequisites.json`,
`schema55-pre-adoption-closed-backup.json`, `schema55-live-acceptance.{py,log}`,
`schema55-live-provenance-*`, `schema55-live-service.txt` and
`schema55-final-readback.json`. Remaining graph/text, reclamation, agent-contract
and production-volume acceptance work is still unfinished.

## Schema-55 Source and Stream implementation

Source now adds minimal Source/Stream RDF through occurrence metadata ownership.
Formats 66–76 fence older writers; the underlying development authority family
maps format 59 to 70. Startup migrates actual record/artifact graph owners,
including private prepared imports, in resumable single-owner atomic batches.
Native term codecs, builders and the original dictionary reference writer remain
the only RDF implementation. Migration validates graph slots, dictionary identity
and exact native term closure before replacing ownership.

Source and Stream facts deduplicate in the default union while retaining named
occurrence contexts. A record pin preserves them after parent and registry expiry;
ordinary imports invent no Stream. Metadata-layer exclusion preserves membership.
Private record preparation now performs data admission at its final atomic write
boundary, after progress and authority metadata have joined the batch.

Signed source `01626f1a7b39efb14d7e215b3b9bb9ac935f89ab` passed its normal
full commit hook: release Clippy with warnings denied, all workspace tests
(including 25 core unit, 183 storage and 64 RPC tests), census self-tests and
280 pinned ownership jobs with zero exceptions. Tests cover default-union
deduplication, named occurrence contexts, all three stream origin kinds,
metadata-layer exclusion, private graph migration, corrupt ownership and missing
terms, resumable reopening, and provenance lifetime under pins and retirement.

A synchronized, hash-verified 112-file copy of the closed schema-54 qualification
store migrated from format 59 to 70 through the existing offline recovery example.
No listeners or intake workers were started. All 217 records and 36 complete
artifacts retained identical byte lengths and native content digests across 253
targets and 36,005,590 logical read bytes, including artifact/record overlap.
Native RDF inspection retained all other projection/diagnostic fields and counts;
metadata primary rows increased by 2 for 195 records, 5 for 9 producer records,
and 6 for 13 tail/receiver records. Reopening returned identical target receipts.
Every original qualification-store file hash remained unchanged after both runs.

Evidence under `.stage/native-graph-integration/` is
`source-provenance-commit.log`, `schema55-private-copy.{py,json}`,
`schema55-recovery-build.{jsonl,log}`, `schema55-copy-verify.py`,
`schema55-copy-all-bytes{,-first}.json`, and
`schema55-copy-qualified{,-first}.json`. The explicitly selected recovery image
has SHA-256 `56ec7d1a9c8df4fb2140a5b13db82db5f0773f55e90222860012ff7ed1af32b4`
and the dev-only `test-support` feature; its fixture-retirement entrypoint was not
invoked. These finite offline checks do not qualify an ordinary runtime image.

The later native query, ordinary-runtime and live-adoption qualification is
recorded above. Those finite development checks leave broader acceptance open.
This implementation does not complete text/SPARQL relations, governed public
EXPLAIN, forced bounded physical reclamation or production-volume acceptance.

## Schema-54 live adoption

Signed source `be0994e8a5ee30abf91513b327b460d210244507` passed the normal full
pre-commit gate in `all-artifact-commit.log`. This includes the final write-boundary
adjustment, 24 core unit tests, 182 storage tests, 64 RPC tests and the pinned
census with 279 ownership jobs and zero exceptions.

A hash-verified private copy of the 108-file schema-53 closed backup migrated
from format 48 to 59 without activating listeners or intake workers. Its durable
watch definitions were preserved. An offline probe passed SELECT, ASK, CONSTRUCT
and DESCRIBE; native class counts of 217 Records and 36 complete Artifacts agreed
with independent paged artifact-catalog discovery. Reopening produced identical
query reports. Both old discontinuous generations retained offsets 37/0, records
1/0, root device 56, original revision and terminal expiry, and `identity_changed`.

The existing offline recovery example then verified all 217 retained records and
36 complete artifacts: 253 targets and 36,005,590 logical bytes, including overlap
between artifact and record reads. The original backup's complete hash map
remained unchanged. The first target-list harness incorrectly assumed a single
page; the corrected three-page query consumes and releases the native cursor.
Final live handles/bytes are zero. Evidence under `.stage/native-graph-integration/`
includes `schema54-copy-{source,images,first,reopen,qualified}.json`,
`schema54-copy-build.{jsonl,log}`, `schema54_copy_probe.rs`,
`schema54-copy-all-bytes.{json,log}` and `schema54-copy-targets-*`.

The offline examples compile with the dev-only `test-support` feature. Their
qualification did not invoke its fixture-retirement entrypoint. Ordinary runtime
packaging was separately selected with
`cargo build --locked --release -p sparrow-server --bins --message-format=json`;
the emitted core feature list is empty. Its machine artifact paths selected:

- `~/.local/libexec/sparrow/sparrowd-schema54-be0994e`, SHA-256
  `42020c6249e33103b587d99b54b7a8067a1caf2f2228901ef926fc9205ce78c5`.
- `~/.local/libexec/sparrow/sparrow-schema54-be0994e`, SHA-256
  `000220d0e430b3984453a82c8a674fc6fdc960947c8e1c25706d88d25b7b564d`.

A fresh private daemon using that exact ordinary pair passed CLI/MCP diagnostic
matches and exclusions, complete original artifact byte length, metadata-layer
exclusion, record-only selection without implicit whole-parent membership, strict
unknown-artifact refusal without a successful prefix, and permissive incomplete
coverage. It shut down cleanly with zero graph handles/bytes. These receipts use
the `schema54-agent-*` prefix. The temporary probe example was removed from source.

Both installed image hashes and the daemon's sole `cap_net_bind_service=ep`
capability were checked before SIGINT drain. With the unit inactive, PID zero
and Unix socket absent, an exclusive native lock protected a fresh synchronized
110-file closed backup at `schema54-pre-adoption-closed-backup-20261018`.
Every regular-file hash matched the closed source before and after copying.
Configuration, unit and intake roots were preserved while both executable aliases
were switched. The enabled unit started at 2026-10-18 08:31:20 UTC, observed PID
4078213, and the first readiness probe succeeded. Installed aliases/hashes,
capability, unit state and typed behavior were checked; no running-process
executable hash is claimed.

Live TCP still binds `0.0.0.0:518`; TCP and Unix agree on schema 54, format 59,
original store identity, 217 records, 213 payload chunks and no watch failures.
All four native read forms, six-page DESCRIBE, cross-endpoint continuation/release
and CLI/MCP agreement passed. Native counts remain 217 Records and 36 Artifacts.
Populated-artifact identity, 96-byte length, format and typed receipt/retention
timestamps agree with the independent pre-upgrade catalog receipt. Metadata-layer
exclusion preserves membership while yielding no graph rows. The count query
admits 7,568,623 source bytes at unchanged ceilings; accounted peak live bytes
remain 115,740,672. These are logical accounts, not physical I/O or RSS results.

Both recovered generation receipts are exactly equal to their schema-53 receipts
over TCP and Unix, including original declarations, clocks and gaps. No recovery
was repeated and no input was ingested by live acceptance. Final graph handles
and bytes are zero; the fresh closed backup hashes remain unchanged. Evidence
includes `schema54-runtime-{build,artifacts}*`,
`schema54-adoption-prerequisites.json`, `schema54-pre-adoption-closed-backup.json`,
`schema54-readiness-*`, `schema54-live-acceptance.{py,log}`,
`schema54-live-populated*`, `schema54-live-{tcp,unix}-generation-*`,
`schema54-live-service.txt` and `schema54-final-readback.json`.
This finite development qualification leaves the remaining graph, reclamation,
agent-contract and production-volume requirements below unfinished.

## Populated-artifact RDF source qualification

Source schema 54 adds independent populated-artifact metadata, source profile v3
and store writer fences 55 through 65, adopted above. Direct publication shares the
record RDF writer; staged publication adds the parent only after complete-byte
validation. The native streaming manifest header supplies scalar metadata and
effective eviction state without materializing its arrays. Artifact timeline and
graph retirement share the existing native reference owner.

The first release sweep passed 175 storage tests and exposed five failures in
`all-artifact-tests.log`. Record-only query fixtures and explicit-parent scope
expectations were adjusted for independent artifact membership. The pressure
failures exposed an earlier, smaller preliminary admission estimate: direct
ingestion now admits at the final native write boundary after source-context and
authority metadata join the batch, retaining the greater conservative estimate.
The release sweep `all-artifact-full-check.log` passed 24 core unit tests,
182 storage tests, 64 RPC tests, the other workspace suites and the seeded census
self-tests. The new staged/private-publication, record-pin lifetime,
upgrade-resume and missing-checkpoint regressions passed. The pinned census held
279 ownership jobs with zero exceptions. Final-source verification, including the
last write-boundary adjustment, is the normal hook in `all-artifact-commit.log`.

Historical fixture cleanup uses a dev-only feature entry into the same native
RDF retirement implementation. Ordinary runtime builds must omit that feature.
Private-copy migration acceptance and ordinary runtime packaging/adoption are
recorded above.

## Source catalog admission qualification

Schema 53 source identifies `sparrow-rdf-source-v2`: pinned catalog bodies are
admitted before decoding, separately from bounded borrowed temporal-index rows.
The old 512-KiB per-candidate allowance exhausted 128 MiB after 256 visits in a
300-record, 900-byte JSONL input. The failing focused receipt is
`.stage/native-graph-integration/rdf-catalog-io-before.log`. Source/request/row,
catalog-memory and scan ceilings have not been raised. The replacement passed
`make check`: release Clippy with warnings denied, 23 core unit tests, 180 storage
tests, 15 server unit tests, 64 RPC tests, the other workspace suites and seeded
census self-tests. The pinned cross gate reports 278 ownership jobs and zero
distinct exceptions. The full receipt is `rdf-catalog-io-full-check.log` in the
same evidence directory. Both the 300-record and 300-empty-artifact native count
regressions pass at unchanged defaults; CLI and MCP preserve the 300-record
count and advertise source profile v2. Missing or unknown profiles refuse.
The final focused counter receipt `rdf-catalog-io-final-focused.log` reports
2,108,461 admitted source bytes for 300 records and 21,480,711 for 300 empty
artifacts; both report 34,639,872 peak accounted live bytes. These are logical
admission counters, not physical I/O or RSS measurements.
The qualified source is now adopted below.

## Schema-53 live adoption

Signed source `4d44a2e2dbe4c8a15473eeae436e9afac447ecba` passed the normal full
pre-commit gate, recorded as `rdf-catalog-io-commit.log` under
`.stage/native-graph-integration/`. Both runtime bins were then explicitly built
with `cargo build --locked --release -p sparrow-server --bins --message-format=json`.
Cargo's machine artifact paths selected these exclusive installed files:

- `~/.local/libexec/sparrow/sparrowd-schema53-4d44a2e`, SHA-256
  `aebc1fefbcdd586f8a84ce62d6e4b1001d4c8767b39c81a1752cba257375e560`.
- `~/.local/libexec/sparrow/sparrow-schema53-4d44a2e`, SHA-256
  `3ab8f1819ab1dab3126f4987ee358e421577aaa011b22682f0a2c7b9eb285cf0`.

The daemon's sole `cap_net_bind_service=ep` capability and both image hashes were
verified before draining the predecessor through SIGINT. With the unit inactive,
PID zero and the Unix socket absent, an exclusive native store lock protected
the 108-file closed copy `schema53-pre-adoption-closed-backup-20261018` in that
evidence directory. Every regular-file hash matched the closed source before
and after copying. The backup and manifest were synchronized before switching
both executable aliases. Configuration, unit and intake roots were preserved.

The enabled persistent unit started at 2026-10-18 07:21:04 UTC, observed PID
3543539. The first readiness probe succeeded. Installed aliases/hashes,
capability, unit state, startup journal and typed behavior were checked; no
running-process executable hash is claimed. TCP still binds `0.0.0.0:518`.
TCP and Unix status agree on schema 53, format 48, original store identity,
217 live records, 213 payload chunks and no watch failures.

Read-only live acceptance passed SELECT, ASK, CONSTRUCT and six-page DESCRIBE,
including continuation across TCP/Unix and release. CLI and MCP agree on the
native count and source profile v2. Full class counts remain 217 Records and one
Artifact. The same native count request returned identical rows while admitted
source bytes fell from 119,681,897 to 5,127,039. Accounted peak live bytes remain
115,740,672; these are neither physical I/O nor RSS measurements. No source or
native governor ceilings were raised. Final graph handles/bytes are zero.

Both original recovered generations still report `identity_changed`, offsets
37/0, records 1/0 and unknown continuity over both endpoints. No recovery was
repeated and no input was ingested by these acceptance calls. Evidence includes
`schema53-runtime-build.{jsonl,log}`, `schema53-runtime-artifacts.json`,
`schema53-adoption-prerequisites.json`, `schema53-pre-adoption-closed-backup.json`,
`schema53-readiness-*`, `schema53-live-service.txt`,
`schema53-live-acceptance.{py,log}`, `schema53-live-query-*`,
`schema53-live-admission-comparison.json`, `schema53-live-mcp.json` and the
`schema53-live-{tcp,unix}-generation-*` receipts. This finite development
acceptance does not complete the remaining graph, reclamation, agent-contract
or production qualification requirements below.

## Schema-52 live adoption

Signed source `7e1b208b26038d13b7023da372355de602fdef3c` passed the normal
pre-commit gate described below. A fresh private copy of the 107-file closed
format-37 backup then replayed migration against that exact source, passed all
four native read forms, retained a Record count of 217, and admitted one private
empty artifact with zero records. The original backup hashes remained unchanged.
Source and probe-image hashes are `schema52-final-restore-source.json`; the
results are `schema52-final-restore.{json,log}` under
`.stage/native-graph-integration/`. The temporary example was removed.

Both runtime bins were explicitly rebuilt after verification with
`cargo build --locked --release -p sparrow-server --bins`. Cargo's machine artifact
paths selected these installed files:

- `~/.local/libexec/sparrow/sparrowd-schema52-7e1b208`, SHA-256
  `61f2810243131cfb9d0bae6b3478ddae94b6a4f1cb570d84630be12efba9bdc1`.
- `~/.local/libexec/sparrow/sparrow-schema52-7e1b208`, SHA-256
  `e27ef0a8a8199e9f6b1ee6e4302a2424d79d7d142cd5c77aabb4bb155f096707`.

The daemon's sole `cap_net_bind_service=ep` capability was verified before
draining its predecessor through SIGINT. With PID zero, an inactive unit and
the Unix endpoint removed, an exclusive native store lock protected a fresh
closed backup at
`.stage/native-graph-integration/schema52-pre-adoption-closed-backup-20261018`.
Every regular-file hash matched the closed source. Configuration, unit and intake
roots were preserved. Stable executable aliases were switched to the qualified
pair, and the unit started at 2026-10-18 06:27:44 UTC, observed PID 3123221.
Installed aliases/hashes, enabled/active unit state, startup journal and typed
behavior were checked. No running-process executable hash is claimed.

The first acceptance call arrived before listener readiness and received
connection refused. It was retained as
`schema52-live-initial-readiness-failure.log`; the same process became ready
without another restart. Native socket inspection then confirmed `0.0.0.0:518`.
Typed TCP and Unix status reported schema 52, format 48 and the original store
`store:46c88e949ccec658600b66c7becd5bb7`, with 217 records, 213 unique payload
chunks and no watch failures. SELECT, ASK, CONSTRUCT and six-page DESCRIBE passed
at default limits, including continuation across the two endpoints and release.

Live acceptance then durably ingested one zero-byte synthetic log artifact from
the private fixture file, source `development-schema52-empty`, request
`development-schema52-empty-7e1b208`. Its native metadata preserved the occurrence
IRI, zero typed byte length, format and exact typed receipt/retention timestamps.
CLI and Unix MCP agreed on one selected artifact and zero selected records.
Metadata-layer exclusion retained known membership while yielding no graph rows.
A service diagnostic filter yielded no rows and one unknown artifact in
permissive mode; strict mode returned MCP `isError=true`, `INDEX_NOT_READY`.
Full native class counts after ingestion were 217 Records and one Artifact.
Final status retained 213 chunks and zero graph result handles/bytes. The one
pre-existing failed content projection remains independent coverage evidence.

Fresh typed reads also preserved both recovered generations' `identity_changed`
gaps, offsets 37/0 and record counts 1/0. This deployment did not repeat recovery
or claim restored producer continuity. Runtime receipts include
`schema52-runtime-build.{jsonl,log}`, `schema52-runtime-artifacts.json`,
`schema52-adoption-prerequisites.json`,
`schema52-pre-adoption-closed-backup.json`, `schema52-live-service.txt`,
`schema52-live-acceptance.{py,log}`, `schema52-live-empty-*`,
`schema52-live-*-count-after-empty.json` and `schema52-recovered-generation-*`.
This is finite development acceptance; the remaining graph/reclamation and
production qualification requirements below are still unmet.

## Recordless artifact RDF source qualification

The implementation branch adds schema 52 and store formats 44 through 54 for
recordless artifact RDF, now adopted above. The source shares the native RDF writer,
occurrence metadata envelope, catalog migration pager and captured query scope.
The full local gate passes release Clippy with warnings denied, 21 core unit
tests, 179 storage tests, all 63 RPC tests, the other workspace/doc tests, census
self-tests and 277 pinned ownership jobs with zero distinct exceptions.
Qualification includes interrupted owner backfill, reopen/idempotence, required
checkpoint failure, all writer-family transitions, native class identity,
source/time filters, diagnostic unknowns, pin/snapshot retention, corruption
refusal, bounded expiry and independent CLI/MCP artifact coverage.

Migration and direct empty publication share the original write-batch estimate
and linked-file admission law. Tests refuse impossible/pressured migration
without advancing its checkpoint, refuse an already present owner outside its
atomic checkpoint, and resume after recovery. The direct-ingestion regression
leaves room for the earlier empty-record estimate but refuses the complete RDF
batch before publication; removing its private sparse ballast permits the same
request to succeed once. Sparse ballast qualifies admission accounting, not a
filesystem quota or forced physical reclamation.

A prior hash-verified restored-copy trial migrated the original 217-record store
from format 37 to 48 and passed all four read forms at default limits. Separate
Record and Artifact counts preserved occurrence distinctions. That copy initially
had no empty artifacts; a private synthetic empty input then returned one artifact
and zero records while keeping 217 live records and 213 payload chunks. The 107
closed-backup hashes remained unchanged. This finite trial predates the final
admission hardening; exact-source migration replay is a separate adoption gate.

Evidence under `.stage/native-graph-integration/` includes
`artifact-rdf-admission-final-check.log`, the signed normal commit gate
`artifact-rdf-commit.log`, `schema52-restore-probe.{json,log}`,
`schema52-restore-empty-probe.{json,log}`, and their source/image receipts.
The temporary probe example was removed. Exact-source replay and live adoption
of schema 52/format 48 are recorded above.

## Native graph transport source

The implementation branch now includes schema 51 native graph RPC publication,
CLI `sparql REQUEST.json`, MCP `sparql_query`, and generic graph `result_next`.
These changes are deployed on the development service. The original schema-51
transport preserved the prior store format; schema 52 adds the recordless metadata
fence above. The service remains the sole database writer.

The typed transport preserves native complete/certain/at-most/unknown outcomes,
all governor dimensions, source evidence, coverage, RDF 1.2 graph item roles,
page-local term forests, native diagnostic locations/presentation arguments,
and exact 128-bit storage-capacity values. CLI/MCP use PurRDF's result serializer
and canonical term projection, with explicit empty graph declarations and SELECT
auxiliary graphs beside the native JSON result envelope. Query failures preserve
the receipt while setting MCP `isError` and an unsuccessful CLI exit status.

Qualification covers scoped blanks/directional triple terms, compact-DAG
amplification and unreachable-node refusal, native binding validation independent
of JSON member order, and diagnostic fidelity including capacity near
`u128::MAX`. Private-daemon acceptance covers all four read forms, typed bindings,
TCP CLI and Unix MCP replay, certain/at-most/unknown certificates, native query
failures, unsuccessful CLI exits, release and cyclic-wire refusal. Single-page
results do not retain a continuation handle; releasing those returns false.

The graph transport's original pinned census held 272 ownership jobs with zero
distinct exceptions; scoped cursor ownership adds one job in the hardening below.
Event fields and capacity diagnostics share the SDK's original `WideInteger`
carrier and codec; argument errors and count rendering also reuse their existing
owners. No detectors were relaxed. The final normal `make check` commit gate is
logged in `.stage/native-graph-integration/graph-wire-commit.log`; the related
focused, integration and census receipts use the `graph-wire-*` prefix.

Source/Stream class declarations, native text
relations, correlation, public
explanation, forced bounded physical reclamation and production-volume/resource
acceptance remain unfinished. The concrete native storage integration gap is
documented in [native reclamation integration](native-reclamation.md).

Graph egress hardening reproduced two adapter failures before their fixes. A
241-node native triple term fitted the forest's 512-node/256-KiB admission but
failed a separate generic JSON depth default. The renderer now derives JSON
bounds from transport admission; native serialization and native readback retain
the complete term and an unbound cell. A 100-row projection with a 5,000-character
variable name exposed a separate per-page sizing omission: native SRJ repeats
the name in each row. Core boundary pricing now includes that repeated charge.
The MCP/CLI regression traverses bounded pages and uses the native result reader
to verify all 100 original values. Before/after receipts are
`deep-result-{before,after}.log` and `projection-name-{before,after}.log` under
`.stage/native-graph-integration`.

Source-read refusal diagnostics also identify attempted requests, admitted bytes
and row visits beside their ceilings. Failed requests retain the original sticky
error and leave admitted-consumption counters unchanged.

A hash-verified private copy of the pre-recovery development backup exposed a
storage access problem absent from the tiny synthetic queries. Its single-record
DESCRIBE exhausted 100,000 requests after 98,510 row visits, despite admitting
only about 15.9 MB. Unbound reads scanned foreign statement rows before owner
filtering. They now merge the existing graph-prefixed cursors for selected graph
declarations, preserving physical order, owner filtering, empty declarations and
default-union deduplication. The focused storage regression includes 2,000
foreign rows, shared graph names and duplicate selected owners under a 500-visit
ceiling. No governor ceiling or stored format was raised for this repair.
The private copy then completed all four read forms at default limits, including
DESCRIBE with 5,747 requests, 4,019 row visits and 3,668,536 admitted bytes. The
full retained metadata count was 212, matching selected occurrence coverage;
its independent content coverage still reported one failed projection. Those
receipts are `schema51-restore-probe-after.json`/`.log`. The subsequent lazy
initialization change prevents graph reads merely from constructing an unused
iterator; its regression checks zero added requests, rows and bytes before a
pull. The full hardening gate and signed commit receipts are
`graph-hardening-full-check-v2.log` and `graph-hardening-commit.log`.

## Schema-51 live adoption

Signed source `373b715771ecd10adcfeedbafd845d1707195046` passed the normal
pre-commit `make check` gate: release Clippy, all release tests (including 19 core
unit tests, 175 storage tests and 62 RPC tests), census self-tests, and 273 pinned
ownership jobs with zero distinct exceptions. Both runtime executables were then
explicitly rebuilt with `cargo build --locked --release -p sparrow-server --bins`;
Cargo's machine artifact paths selected the installed bytes.

The daemon image is
`~/.local/libexec/sparrow/sparrowd-schema51-373b715`, SHA-256
`3224b16d89e5dddd1d29e35757c1b66b9770e84715925f10cd45cecb06cc54d4`.
The CLI image is `~/.local/libexec/sparrow/sparrow-schema51-373b715`, SHA-256
`cb1a631d80cf9dfd3479365c2b63f6aa0b1226ab59f66982c1a880f36e0eb580`.
The daemon's sole file capability `cap_net_bind_service=ep` was verified before
stopping its predecessor. Stable aliases resolve to these qualified hashes;
configuration, unit and intake roots were preserved. The enabled persistent unit
was drained through SIGINT. With PID zero and the Unix endpoint removed, an
exclusive native store lock protected a fresh closed backup at
`.stage/native-graph-integration/schema51-pre-adoption-closed-backup-20261018`.
Every regular-file hash matched the closed source before switching aliases.
The earlier pre-recovery backup's full hash manifest was independently rechecked.

The service started at 2026-10-18 05:20:30 UTC, observed PID 273940. Service state,
start journal, installed aliases and hashes, schema discovery and typed queries
were verified. Direct executable readback through the process filesystem was
denied by the automatic policy guard, so no running-process executable hash is
claimed. Native socket inspection confirmed `0.0.0.0:518`; typed TCP and Unix
status reported the original store `store:46c88e949ccec658600b66c7becd5bb7`,
schema 51, format 37, 217 live records and 213 unique payload chunks, with no
watch failures.

Live SELECT, ASK, CONSTRUCT and DESCRIBE passed at default governors against an
original retained record. DESCRIBE traversed six immutable pages through TCP
initial execution and Unix continuation: 23 transport items preserve 22 native
quads and one named-graph declaration. It admitted 5,797 requests, 4,069 row visits
and 3,672,376 source bytes. A full retained metadata COUNT returned 217, matching
occurrence coverage; its 119,088,342 admitted bytes are finite-corpus evidence,
not a production capacity claim. The independent failed-content count remains
one. Unix MCP SELECT matched the CLI's native result, and final status reported
zero SPARQL handles and retained result bytes. The probe initially expected 22
transport items, then corrected that assertion to include the declaration and
reran successfully; no runtime change was needed.

Evidence under `.stage/native-graph-integration/` includes
`schema51-runtime-build.{jsonl,log}`, `schema51-runtime-artifacts.json`,
`schema51-adoption-prerequisites.json`,
`schema51-pre-adoption-closed-backup.json`, `schema51-live-service.txt`,
`schema51-live-query-*`, `schema51-live-mcp.json`,
`schema51-live-acceptance.{py,log}`, `schema51-live-acceptance-summary.json`,
and the before/after typed status receipts. This establishes development graph
acceptance on the owned synthetic store. At that schema-51 boundary,
recordless-artifact RDF metadata,
text/correlation/explanation integration, forced bounded physical reclamation,
and production-volume/resource acceptance remain unfinished.

## Device-identity discontinuity and approved recovery

A startup attempt on 2026-10-17 exited with the native guard:
`active file checkpoints require their original watch revision, root and interpretation; restore that definition before recovery`.
The two active generations of the owned synthetic watch
`development-tail-8a45378` captured root device 56, inode 55520248; the current
root reports device 37 and the same inode. Revision 7, source and JSONL
interpretation agree. Matching inodes do not establish producer continuity
across a changed device identity.

Generation `file-generation:91d7d7c4e3e638f9e0de28992a37fae9` has offset 37
and one accepted record; `file-generation:b0236e4886ec59daabdadc86a0267088`
has offset zero and no records. The original declarations and checkpoints
were initially left unchanged. A read-only native RocksDB inspection verified that every
regular file in the source store had the same SHA-256 before and after inspection.

A private-copy trial used the existing `tail_discontinue` API with
`identity_changed`. It preserved both declarations, offsets, record counts and
original start clocks, and left live-record/chunk/projection/pin counts unchanged.
This would permanently end the old generations. A subsequent watcher run may
read the current files as new occurrences; it cannot claim uninterrupted capture.
The trial did not change the source store or start watchers/listeners.

After explicit approval, a second private-copy trial verified the guarded helper
twice, including an idempotent retry without renewing terminal expiry. The source
store's regular-file fingerprints remained unchanged. Before applying recovery,
an exclusive native store lock protected a complete closed copy at
`.stage/native-graph-integration/runtime-pre-recovery-backup-20261017`; its
regular-file SHA-256 map matched the source exactly.

Recovery was applied at UTC milliseconds `1792290770987`. Both old generations
now declare `identity_changed`; declarations, offsets, accepted record counts
and start clocks were preserved. Live records remained 212, payload chunks 213,
JSON projections 208 and active pins zero across that operation. The persistent
unit then started successfully. Typed status over TCP and the private Unix socket
reported the original store `store:46c88e949ccec658600b66c7becd5bb7`, schema 50
and format 37. Native socket inspection confirmed the listener at `0.0.0.0:518`.
This closes the old generations; it does not certify uninterrupted producer
continuity or exclude future re-ingestion of files still present in the watch.

The watcher subsequently created four new generations, accepting 1, 1, 3 and 0
records from the present files. Typed status then reported 217 live records and
the same 213 payload chunks. These five new occurrences are the approved
re-ingestion possibility becoming observable; they do not renew or rewrite the
old generations. Both old checkpoints were independently fetched over typed TCP
RPC and still report offsets 37/0, records 1/0 and `identity_changed`.
The source inventory preserves `continuity_unknown=true`. Further evidence is
`runtime-recovered-status-after-watch.json`,
`runtime-recovered-tail-source.json` and the `runtime-recovered-generation-*`
receipts in the same evidence directory.

Local evidence is under `.stage/native-graph-integration/`:
`runtime-start-failure.log`, `catalog-probe-run.json`,
`checkpoint-trial.log`, `checkpoint-trial-result.json`,
`checkpoint-recovery-qualified.json`, `runtime-pre-recovery-backup.json`,
`checkpoint-recovery-apply.log`, `runtime-recovered-status.json` and
`runtime-recovered-unix-status.json`.
At this recovery boundary the daemon remained `28e3915`; later schema-51
adoption is recorded above. Neither a full `make check` pass nor copy-only
recovery alone establishes live service availability; the separate
post-recovery typed status calls do.

## Independent backup restoration

The closed predecessor backup from the schema-50 adoption was independently
copied and opened with the existing offline recovery example built from signed
source `a42009f`. Qualification evidence is
`~/.local/state/sparrow/schema50-restored-backup-5bcy8s11`, including the restored
store, exact image, configuration, driver, captured live inventory, original
backup fingerprints, native report and syscall trace. The recovery image has
SHA-256 `ab2ba61a3b4b577bc5f72eafdfc449def3972e34086b7592c5c5088725d4969b`.
Build/run logs are `/tmp/sparrow-schema50-recovery-build.log` and
`/tmp/sparrow-schema50-restore-qualification.log`.

At the explicit eligibility clock `2026-10-09T05:39:11.059Z`, the checker verifies
all 212 retained records, 32 complete artifacts and three producer-finished file
generations. Its 247 unique evidence targets cover 36,005,499 original bytes;
overlapping record, artifact and generation views deliberately count their reads
separately. Native chunk size/digest checks cover every read, artifact digests
match original receipts, byte lengths match the captured live inventory and every
record's native RDF inspection succeeds with its original store/record identity.
The two unfinished generations are excluded from complete-file recovery targets.
This is restoration of the recorded closed backup, not a new crash/power-loss
experiment or a proof that every internal catalog invariant has been audited.

The configuration points only to the private restored store. The example creates
no intake worker, daemon, maintenance loop or socket. The syscall trace confirms
no bind/listen/connect operations and confines native unlink/rename activity to
the restored directory. Full SHA-256 fingerprints prove the backup remains
unchanged. Complete public inventory captures are released; the enabled active
service retains PID 4081797, store identity, 212 records, payload references,
projections, pins and zero result leases. No public intake or source retirement
occurs. This qualification leaves the original backup intact and does not certify
a live service rollback or the older historical backups below.

## Logical retention accounting

Signed implementation `982f105` and CLI usage follow-up `28e3915` expose native
logical retention accounting through typed `retentionMetrics`, the certified
SDK, CLI `retention-metrics STORE_ID` and MCP `retention_metrics`. The complete
admitted read reports eligible records/artifacts, oldest surviving receipt,
effective window and unique raw payload ownership. Pins, ordinary eligibility,
private preparation and partial manifest retirement retain their existing native
owners. Shared chunks are counted once; a live, pinned or unclassified owner
prevents a chunk from being reported as logically retirable. The shared payload
reader admits borrowed bytes before hashing and checks native size/digest before
creating a caller-owned copy. Physical files and index bytes remain separate
from this logical raw-payload account. See the
[metric contract](interfaces.md#bounded-logical-retention-metrics) for exact
categories, overlap and admission limits.

Private qualification covers shared content, record-only and empty-artifact
pins, thirty-day expiry, release, restart, unpublished private preparation,
reference/payload corruption, independent work/byte refusals and
cancellation. CLI/MCP compare certified results and strict store/input/budget
errors. The full gate and both normal signed-commit hooks pass 167 storage
scenarios, 60 RPC scenarios, 20 SDK tests, the remaining workspace tests, release
Clippy, seeded census and 258 ownership jobs with zero distinct exceptions.
Logs are `/tmp/sparrow-retention-metrics-gate.log`,
`/tmp/sparrow-retention-metrics-commit.log` and
`/tmp/sparrow-retention-metrics-usage-commit.log`. Five explicitly selected runtime
images are frozen at `~/.local/state/sparrow/receiver-history-images-xolyszjx`.

Adoption evidence is `/tmp/sparrow-retention-metrics-live.uxc9qyp9`, including the
closed-store backup qualified by [independent restoration](#independent-backup-restoration).
The enabled active
unit runs PID 4081797 from `sparrowd-schema50-28e3915`, SHA-256
`d71f06fb4018df986cfd424ee2096e4c4f9c8af3fa216f383a11b205a8e2453f`,
with `cap_net_bind_service=ep`. The stable CLI selects
`sparrow-schema50-28e3915`, SHA-256
`4a7b071727c3457acee7ec30d177cb97896e4fbcf98a81305d649b4d71ef5ed8`.
Frozen, installed and alias hashes agree. Saved intents and eight subsequent
live readback flows pass without new public intake or retirement. Metrics agree
over LAN/Unix CLI/MCP with 212 records, 32 artifacts, 213 unique chunks and
307,808 owned/live raw bytes; pin-held, retirable and unclassified raw bytes are
zero. The sampled oldest eligible receipt is `2026-10-07T06:16:07.367Z`;
the account at `2026-10-09T05:32:13.868Z` reports a 170,166,501 ms window and
1,426 admitted visits. This is a finite development-store observation, not a
thirty-day production density or reclamation measurement. Native budget/store/
input refusals, unchanged payload references/projections, saved combined intake
status, original tail clocks, query witnesses, text scores and the exact
35-tool inventory are qualified. Final readback retains format 37, 212 records
and zero pins/result leases. Logs use `/tmp/sparrow-retention-metrics-*.log`.
Complete governed SPARQL admission, forced bounded physical reclamation,
remaining operational metrics and production qualification remain unmet.

## Combined ingestion status

Signed source `218bf5f` supplies one bounded read of raw staging, private
preparation, original acceptance, JSON/RDF/text coverage and synchronized source
retirement. The typed `ingestStatus` operation, certified SDK response, CLI
`ingest-get` and MCP `ingest_get` share the same implementation. Select exactly
one import ID, original intake request ID or original handoff request ID.
Direct intake has no import identity. Tail-only acknowledgement reports the
original terminal generation and zero newly ingested records. Inspection never
advances intake, flushes buffered acceptance, removes a source or extends
retention. Status admission defaults to 10,000 record/pin visits and 32 MiB of
catalog construction, with an explicit maximum of 100,000 visits. Exceeding
admission or encountering contradictory native state refuses the whole read.

Private tests cover preparation, buffered acceptance without synchronization,
corrupt publication/projection state, managed ownership and removal, expired
evidence, tail-only completion, direct intake and upload crash/restart. SDK,
CLI and MCP compare the same immutable state. The full gate and normal signed
commit hook pass 165 storage scenarios, 60 RPC scenarios, 19 SDK tests, the
remaining workspace tests, release Clippy, seeded census and 255 ownership jobs
with zero distinct exceptions. Logs are
`/tmp/sparrow-combined-ingest-gate-v4.log` and
`/tmp/sparrow-combined-ingest-commit.log`; the commit hook includes the final
PurRDF canonical-hex decoder adoption. Five explicitly selected images are frozen
at `~/.local/state/sparrow/receiver-history-images-x8qrxyy1`.

Adoption evidence is `/tmp/sparrow-combined-ingest-live.ptuxy2jn`, including a
closed-store backup that has not independently been restored. The enabled active
unit runs PID 3844791 from `sparrowd-schema49-218bf5f`, SHA-256
`38d9b4cfd87252368dd6ad0e051fb4c38c060cd9ae75322566ab8ac3a16275ee`,
with `cap_net_bind_service=ep`. The stable CLI selects
`sparrow-schema49-218bf5f`, SHA-256
`10a1d542196b64e90bfb17f08f8ed5fef2bbbb15be948c4f79f85045f399149c`.
Frozen, installed and alias hashes agree. Saved intents and seven subsequent
readback flows pass without new public intake or retirement. Combined status
agrees over LAN/Unix CLI/MCP across all three namespaces, retaining the original
tail checkpoint, clocks and removal receipt. Strict ambiguous/null/unknown input,
missing identities and wrong stores refuse. The exact 34-tool inventory retains
read-only status annotations. Live queries preserve source inventories, temporal
field witnesses and native text scores. Final readback preserves store identity,
format 37, 212 records and zero pins/result leases. Logs use
`/tmp/sparrow-combined-ingest-*.log`. Complete governed SPARQL admission,
bounded physical reclamation and production operational acceptance remain unmet.

## Agent upload checkpoints

Historical signed source `83095eb` exposes the existing typed `importGet` operation through
`sparrow_client::get_import_progress`, CLI `ingest-get STORE_ID IMPORT_ID` and
MCP `ingest_get`. One SDK decoder certifies durable offsets and the consistency
of sealing, layout and publication; resumable uploads use that same owner.
Handoff and upload publication references share the existing bounded optional
identity decoder. One agent renderer serves CLI and MCP. The upload checkpoint
neither advances intake nor certifies source deletion. The combined request-ID,
projection-state and deletion-status contract remains unfinished, as described
in [interfaces](interfaces.md#read-only-upload-progress).

The existing private crash/restart scenario now compares SDK, Unix/TCP CLI and
MCP checkpoints at empty, partial, reopened, laid-out and published stages.
Committed prefixes and original deadlines survive daemon crashes, publication
references the original receipt, reads do not change progress, and the original
source bytes remain intact. Wrong-store, unknown canonical import IDs and unknown
tool fields refuse; tool inventory and read-only annotations are checked.
SDK tests also reject inconsistent offsets, seal/layout/publication states and
changed response identities. The final full gate and normal signed-commit hook
pass 162 storage scenarios, 60 RPC scenarios, 18 SDK tests, remaining workspace
tests, release Clippy, seeded census and 250 ownership jobs with zero distinct
exceptions. Logs are `/tmp/sparrow-import-status-gate-v3.log` and
`/tmp/sparrow-import-status-commit.log`. Five explicitly selected images are
frozen at `~/.local/state/sparrow/receiver-history-images-dzl0g7zv`.

Adoption evidence is `/tmp/sparrow-import-status-live.qqlmkccd`, including a
closed-store backup that has not independently been restored. The enabled active
unit runs PID 3185010 from `sparrowd-schema48-83095eb`, SHA-256
`c936646c75dd9cb50628380c18114974f05f3634a008cb7bc7ee97e1b9f64684`,
with `cap_net_bind_service=ep`. The stable CLI selects
`sparrow-schema48-83095eb`, SHA-256
`112d50e5ba56de88eb2cd4a3d8ff3a2fd145eb775925012f958618e67ed09458`.
Frozen, installed and alias hashes agree. Saved intents and seven further live
readback flows pass without new public intake or retirement. The new checkpoint
flow verifies TCP/Unix CLI/MCP native missing-import/store-mismatch failures,
strict input and the exact 34-tool inventory. Positive checkpoint stages are
qualified on private daemon instances, not by adding public data. Final LAN
readback preserves store identity, schema 48, format 37, 212 records and zero
pins/result leases. Logs use `/tmp/sparrow-import-status-*.log`. Full graph-query
integration, bounded physical reclamation, combined ingestion status and
production operational acceptance remain unfinished.

## JSON capture admission

Signed source `af09657` routes JSON captures through the same host-owned disk
item admission as chronological record captures: the requested scan budget,
at most 100,000. Native record and JSON occurrence visit limits are unchanged;
the default remains 10,000. Ranking and source-time ordering retain their
separate 10,000-item ceiling. Profile `sparrow-query-capture/6` identifies this
change. Disk staging remains at most 64 MiB, pages 256 KiB and individual rows
32 KiB. Key aggregation retains a charged 32 MiB working allowance, now also
enforced for direct nonpaged calls. This does not admit arbitrarily large scalar
values or qualify production peak memory and ingestion rates.

The full gate and normal signed-commit hook pass 162 storage scenarios, 60 RPC
scenarios, remaining workspace tests, release Clippy, seeded census tests and
248 ownership jobs with zero distinct exceptions. Source evidence is
`/tmp/sparrow-json-disk-index-gate-v3.log` and
`/tmp/sparrow-json-disk-index-commit.log`. Five explicitly selected runtime images
are frozen at `~/.local/state/sparrow/receiver-history-images-yzjcagfs`.

A new private Btrfs store qualifies the frozen executable against 251 JSONL
records generated by the shared native dev helper, including duplicate members.
The 2,725,532-byte fixture yields 12,502 key/kind rows in 77 pages and 12,502
matching string occurrences in 108 pages. Complete CLI/MCP continuations agree,
receipts remain frozen, native paths and occurrence/record counts match, and
216 sampled spans reconstruct the exact original bytes. Resident lease metadata
is 3,877 bytes for keys and 3,889 for values. Default occurrence admission refuses
without a page or handle; explicit scan admission succeeds. Release leaves zero
pins and result leases, and SIGINT closes the daemon successfully. Evidence is
`~/.local/state/sparrow/json-disk-index-qualification-uk0cysdj`, including the
driver, generator source/lock identities, replies and process counters. Kernel
VmHWM is 95,420 KiB; write_bytes is 248,668,160 across ingestion, maintenance and
staging, not an isolated amplification measurement. Closed files total
13,723,261 bytes (13,750,272 allocated). This is a finite synthetic workload.

Adoption evidence is `/tmp/sparrow-json-disk-index-live.hiqtvd7o`, including a
closed-store backup that has not independently been restored. The enabled active
unit runs PID 2910938 from `sparrowd-schema48-af09657`, SHA-256
`063268eae32d55c14ac03493caf21b0642bb8d816ae6a8c2fd9312b85f15808e`,
with `cap_net_bind_service=ep`. The stable CLI selects
`sparrow-schema48-af09657`, SHA-256
`ea4eff97482d373e2ee1abb80d57b4a7050df17a2a40feee40595923efdb028b`.
Frozen, installed and alias hashes agree. Saved intents and six further readback
flows pass over LAN TCP, Unix, CLI and MCP, including temporal/field/native-score
witnesses, 21 sources, five generations, grouped captures and keys/values.
Final TCP status preserves store identity, schema 48, format 37, 212 records and
zero pins/result leases. Logs use `/tmp/sparrow-json-disk-index-*.log`. Full
SPARQL, larger ranked and scalar workloads, and production operational
acceptance remain unfinished. The older capture evidence below is historical.

## Disk-backed capture indexes

Signed source `e4b9500` persists each result's offset/length pair alongside its
anonymous row data, instead of retaining all offsets in memory. One bounded
page map freezes byte/item boundaries once; replay uses that map and reads only
the admitted page's index entries and rows. The same page admission owner serves
memory replies and the disk map. Index I/O and invalid ranges fail without a
successful response. Source-time ordering rewrites this index under its existing
charged sort admission; native scoring and JSON semantics remain unchanged.

Chronological record captures now use the requested candidate scan budget as
their item ceiling. The maximum scan budget stays 100,000. Ranked, source-time
and JSON captures retain their separate 10,000-item boundary. Disk staging stays
at most 64 MiB, resident result admission 2 MiB per capture/8 MiB aggregate,
eight handles, 256 KiB pages and 60-second absolute leases. Both anonymous files
are charged together, including 128 KiB allocation overhead within that disk
reservation. Host capture profile `/5` identifies the changed execution path;
schema 48 and catalog format 37 remain unchanged.

The full gate and normal signed-commit hook pass 162 storage scenarios, 60 RPC
integration scenarios, remaining workspace tests, release Clippy, seeded census
tests and 248 helper ownership jobs without distinct exceptions. The new large
fixture uses the production streaming import owner: 12,003 IDs replay exactly
after retirement and later intake; invalid page boundaries and a smaller scan
budget still refuse. Anonymous staging tests also inject truncated/out-of-range
indexes and failed row reads. Source logs are
`/tmp/sparrow-disk-index-gate-v2.log`, `/tmp/sparrow-disk-index-commit.log` and
`/tmp/sparrow-disk-index-selected-build.log`. All five explicitly selected runtime
images are frozen at `~/.local/state/sparrow/receiver-history-images-4_2bwoa1`.

The retained real corpus now qualifies full chronological traversal of 13,720
pacman records in 94 pages and all 13,868 records in 95 pages, with exact durable
receipt ID membership, immutable receipts and identical CLI/MCP continuations.
Resident lease metadata is respectively 3,876 and 3,858 bytes. The default
10,000-visit request still refuses without a result. Native selective text rows/
scores, complete artifact match counts and unrestricted singleton context also
pass. The private daemon closes cleanly; detailed method and resource limits are
in [sample qualification](real-corpus-qualification.md).

Adoption evidence is `/tmp/sparrow-disk-index-live.71d4n9h4`, including a closed
backup that has not independently been restored. The enabled active unit runs
PID 2527002 from `sparrowd-schema48-e4b9500`, SHA-256
`7145b0a790431ee9b7a164b6830e5be31dd97aa5d6355c8ff888c45c12a93867`,
retaining `cap_net_bind_service=ep`. The CLI alias selects
`sparrow-schema48-e4b9500`, SHA-256
`ea4eff97482d373e2ee1abb80d57b4a7050df17a2a40feee40595923efdb028b`.
Installed, frozen and alias hashes agree. Saved TCP/Unix/CLI/MCP intents, 21
sources, five generations, grouped captures, native scores and temporal/field/
key/value witnesses pass readback without new public intake or retirement.
Final LAN status preserves store identity, schema 48, format 37, 212 records and
zero pins/result leases. Readback logs use `/tmp/sparrow-disk-index-*.log`.
Full SPARQL, larger ranked/JSON workloads and production operational acceptance
remain unfinished.

## Source-local context

Signed source `4bcc201` indexes context by source and chronological identity
instead of decoding unrelated records during neighborhood traversal. The source
timeline is reconstructed with the shared bounded catalog pager on every open,
and maintained in the original atomic record/time publication batch. It remains
a derivative of the existing format 37 catalog. Capture admission profile `/2`
charges same-source visits, including skipped scope members, while preserving
the existing decoding and response ceilings. Corrupt reconstruction disables
context with an actionable failure; unrelated evidence reads remain available.

`make check` and the normal signed-commit hook pass: 161 storage scenarios,
60 RPC integration scenarios, the remaining workspace tests, release Clippy,
seeded census tests and 248 helper ownership jobs with zero distinct exceptions.
Logs are `/tmp/sparrow-source-context-gate-v3.log`,
`/tmp/sparrow-source-context-commit.log` and
`/tmp/sparrow-source-context-selected-build.log`. All five explicitly selected
runtime executables are frozen at
`~/.local/state/sparrow/receiver-history-images-onf5kej2`.

The private 13,868-record corpus now admits unrestricted singleton context for
all four selected Brainstem JSON artifacts without the former time-interval
workaround. CLI/MCP neighborhoods agree and exact bytes match the frozen inputs;
see [sample qualification](real-corpus-qualification.md). The private daemon
exited cleanly after SIGINT. Production-scale cache and startup costs remain
unqualified.

Adoption evidence is `/tmp/sparrow-source-context-live.fauiz3kd`, including a
closed-store backup that has not independently been restored. The active enabled
unit runs PID 2447537 from `sparrowd-schema48-4bcc201`, SHA-256
`3ab58fb2c31b33c7bbb82ff427756f71be28ca72b7428cf3ae23c5de5f102de1`.
The stable CLI selects `sparrow-schema48-4bcc201`; its SHA-256 is
`ea4eff97482d373e2ee1abb80d57b4a7050df17a2a40feee40595923efdb028b`.
Installed/frozen/alias hashes agree, and the daemon retains
`cap_net_bind_service=ep`. Saved TCP/Unix/CLI/MCP intents, all 21 sources, five
generations, grouped pages, native scores and temporal/field/key/value readbacks
pass without new public intake or retirement. Final LAN TCP status preserves
schema 48, format 37, store identity, 212 records and zero pins/result leases.
Readback logs use `/tmp/sparrow-source-context-*.log`. Full architecture
acceptance remains unmet.

## Actionable JSON capture budgets

Real sample investigation reached the default native JSON occurrence ceiling
without a published page. The previous error conflated record, occurrence, row
and byte admission. It now names the exhausted budget and its effective ceiling,
and directs agents to an explicit admitted `scan_budget` or narrower scope.
Default/maximum budgets, paging and refusal semantics remain unchanged. Focused
tests distinguish record visits from occurrences, prove failed captures leave
zero handles/bytes, and then admit the same requests with an explicit budget.
The sample investigation and its actual broad-query/context refusals are recorded
in [local corpus qualification](real-corpus-qualification.md).

Signed source `e28431f` passes `make check` and the normal pre-commit gate:
159 storage scenarios, 60 RPC integration scenarios, remaining workspace tests,
release Clippy and 247 helper ownership jobs with zero distinct exceptions.
Gate/commit/build logs are `/tmp/sparrow-json-budget-gate.log`,
`/tmp/sparrow-json-budget-commit.log` and
`/tmp/sparrow-json-budget-selected-build.log`. All five runtime executables were
explicitly selected after the hook and frozen with source/SHA-256 evidence at
`~/.local/state/sparrow/receiver-history-images-emk0v_7a`.

Public adoption evidence is `/tmp/sparrow-json-budget-live.6kscmjie`, including a
closed-store backup that has not independently been restored. The active enabled
unit runs PID 2341427 from `sparrowd-schema48-e28431f`, SHA-256
`f57d5da4292984396d2a9c577d1c74bff2c589e9db7197c8683642f550cce432`.
The stable CLI selects `sparrow-schema48-e28431f`; installed/frozen/alias hashes
agree. The daemon retains `cap_net_bind_service=ep` and serves `0.0.0.0:518`
with schema 48, format 37 and unchanged store identity, 212 records, healthy
owned watches, zero pins and result leases. Saved TCP/Unix/CLI/MCP handoffs,
exact bytes, all 21 sources, five generations, grouped pages, native text scores,
temporal/field witnesses, four keys and twenty values pass readback without new
public intake or source retirement. Full architecture acceptance remains unmet.

## Joined maintenance scheduling

The implementation branch moves retention, pressure and periodic buffered
synchronization into one joined serial worker. The previous serving loop awaited
each maintenance branch, blocking socket acceptance and signal selection during
that job. It also discarded expiry continuation and waited one second before
every subsequent bounded pass. The worker now awaits one complete native job at
a time, yields between passes, drains expiry continuation and selects eligible
duties fairly. Stop has priority over fresh work. Per-pass storage limits and the
independent RPC/intake request budget remain unchanged. Native writes are joined
before the final RPC drain and WAL fence.

The 1,024-record fixture requires over thirty bounded expiry passes. It completes
under its ten-second functional deadline while retaining the original pinned
record and new buffered bytes, even with a one-millisecond flush interval. Stop
before worker admission leaves buffered work for the final store fence. These
checks establish progress and ownership, not sustained production throughput or
latency bounds. An earlier attempted shared RPC-slot design failed the one-slot
idle-connection contract and was removed; request-budget and connection tests now
pass without changing their acceptance criteria.

Full release verification passes 158 storage scenarios, 14 core unit tests,
60 RPC integration scenarios, seven RPC and five daemon unit tests, SDK and
remaining workspace tests, seeded census self-tests and the pinned cross gate:
247 ownership jobs with no distinct exceptions. Evidence is
`/tmp/sparrow-maintenance-gate-final.log`; the normal signed commit hook is
`/tmp/sparrow-maintenance-commit.log`. Signed source `2f1f8ff` passes the normal
hook. All five executables were explicitly rebuilt and frozen with source and
SHA-256 evidence at `~/.local/state/sparrow/receiver-history-images-5mocitfl`.

Private typed-load qualification
`~/.local/state/sparrow/typed-load-qualification-plug6kp6` completes 32 mixed-format
cohorts with 4,160 records, 8,678,582 original bytes, complete query traversals,
exact-byte checks and released captures. Observed workload time is 10.800230
seconds, sampled high-water RSS 189,608 KiB, closed native files 16,686,057 bytes
and clean SIGINT teardown 0.113505 seconds. Separate active-work evidence at
`~/.local/state/sparrow/rpc-drain-traffic-dr0em_hr` interrupts real typed traffic
after observing native anonymous staging. Shutdown returns success in an observed
0.169069 seconds; same-store reopen verifies 128 records across two complete
artifacts and their exact original bytes. The public service remains unchanged
during both private runs. These observations establish neither sustained
production performance nor worst-case shutdown bounds.

Adoption evidence `/tmp/sparrow-maintenance-live.pweuz1bu` includes a closed-store
backup that has not separately been restored. The enabled active public unit
runs PID 2129972 from `sparrowd-schema48-2f1f8ff`, SHA-256
`e8885b468eb71a2efd6326b3424f3705530c82c15e3f5329a59d7ff5e124e476`.
The stable CLI selects `sparrow-schema48-2f1f8ff`; installed, frozen and alias
hashes agree. The daemon retains `cap_net_bind_service=ep`, serves
`0.0.0.0:518`, and preserves store identity, schema 48, format 37, 212 records,
healthy owned watches, zero pins and read leases. TCP/Unix/CLI/MCP saved intents
preserve receipts, bytes, clocks and replacement files without new public intake
or retirement. All 21 sources, five generation checkpoints, grouped pages,
temporal/field witnesses, native text scores, four keys and twenty values remain
exact. Syslog listeners remain unconfigured. Full governed SPARQL, correlation,
maximum-capacity and sustained turnover, physical reclamation and reboot
acceptance remain unfinished.

## Bounded manifest retirement

The current implementation branch extends maintenance beyond the earlier
8,000-record structural-admission fix. PurRDF's borrowed JSON reader validates
the original artifact catalog and its string arrays. Sparrow retains the encoded
row, scalar metadata and a finite payload prefix rather than building an owned
tree for both large arrays. The existing artifact codec still owns scalar field
semantics. Retry-row expiry uses the same borrowed manifest validation; agent
retries retain their original complete receipt.

Retirement commits a 17-byte original-count, retired-prefix and eviction-state
checkpoint atomically with payload reference changes. Original artifact catalogs
are unchanged. Stored catalog reads resolve effective ownership and availability
through that checkpoint, including artifact discovery. Reopen resumes ownership
retirement without relying on an ephemeral traversal cursor. A refused batch
publishes neither the checkpoint nor the cursor advance. Logical reclamation
does not certify native physical reclamation.

The first checkpoint fences the store at format 42, or 43 with a watch registry.
An explicit feature byte records whether the receiver registry is required.
The shared writer appends this fence after observing native batch publication,
so later watch, receiver, pressure and ingestion writes cannot downgrade it.
Missing or malformed feature/registry state fails. Older installed daemons cannot
open a store after this fence is committed. Wire schema 48 and the PurRDF revision
are unchanged. Existing finite pass limits remain in force; encoded rows and
bounded retained prefixes must fit catalog admission.

The staged 30,000-record fixture passes bounded live traversal, pressure eviction
and expiry while preserving one record pin's exact bytes, followed by complete
logical payload cleanup after release. Its focused run is
`/tmp/sparrow-manifest-large-after.log` (44.14 seconds). Direct publication of this
fixture refuses the existing 64 MiB RDF write-staging ceiling; staged record
preparation exercises an accepted artifact without relaxing that limit. Release
verification covers 158 storage scenarios, 14 core unit tests, 60 RPC integration
scenarios, SDK and remaining workspace tests, seeded census self-tests and the
pinned cross gate: 246 ownership jobs with no distinct exceptions. Checkpoint
tests cover partial retirement across reopen, original root/receipt preservation,
atomic batch refusal, corrupt widths/counts/offsets/flags and registry loss.
Native JSON fixtures cover escaped strings, reordered members and invalid arrays.
Full verification is recorded in `/tmp/sparrow-manifest-gate-complete.log` and the
normal signed commit hook in `/tmp/sparrow-manifest-commit.log`. Signed source
`ba0bd39` passes that hook with the final pinned checkpoint readers. All five
executables were explicitly rebuilt and frozen, including source and SHA-256
evidence, at `~/.local/state/sparrow/receiver-history-images-xngb5jhy`.

Private qualification `~/.local/state/sparrow/typed-load-qualification-p_a9gzzv`
uses that exact daemon for 32 mixed-format cohorts: 4,160 records and 8,678,582
original bytes, complete query traversal, exact-byte checks and released captures.
The workload completes in 15.870464 seconds, with sampled high-water RSS
189,672 KiB, closed native files totaling 16,681,152 bytes and clean SIGINT teardown
in an observed 0.113680 seconds. The public service remains unchanged during this
private run. These are observations, not production throughput, peak retirement
memory or worst-case shutdown bounds.

Adoption evidence is `/tmp/sparrow-manifest-live.9hyfvrn4`, including a closed-store
backup that has not separately been restored. The enabled active unit now runs
PID 1994691 from `sparrowd-schema48-ba0bd39`, SHA-256
`693fcea75fe60ab79eecb6caef811581be76995add66c5f9f150fa3351e7cb44`.
The stable CLI selects `sparrow-schema48-ba0bd39`; frozen, installed and alias
hashes agree. The daemon retains `cap_net_bind_service=ep` and listens at
`0.0.0.0:518`. TCP/Unix/CLI/MCP checks preserve the same store identity, format 37,
212 records, healthy owned watches, zero pins and read leases. Saved intents
preserve original receipts, bytes, clocks and replacement files without new
intake or retirement. The 21-source inventory, five generation checkpoints,
grouped pages, temporal/field witnesses, native text scores, four keys and twenty
values remain exact. Syslog listeners remain unconfigured. Maximum-size manifests,
sustained turnover throughput, physical reclamation, complete governed SPARQL,
correlation and reboot acceptance remain unfinished.

## Structural stored-catalog admission

An accepted 8,000-record manifest exposed a retention failure: multiplying every
encoded catalog byte by 64 refused that first row under the default 32 MiB pass
allowance, leaving expiry unable to traverse it. A regression reproduces this
before the change even while the evidence is live. Stored-catalog decoding now
preflights the encoded slice through PurRDF's borrowed JSON events before
constructing an owned tree. The reservation charges 8 KiB for the bounded native
reader, eight times the encoded length for strings, typed copies and re-encoding,
and 1 KiB per member/value/container-opening event for simultaneous native tree,
vector, uniqueness-set and typed decoding scaffolding. Closing events add no
owned nodes. This is conservative storage admission, not measured RSS.

The complete charge must fit before allocation. Budget failure or native grammar
failure leaves admission unchanged. Native catalog decoding retains its depth,
value-count, string and unique-member limits through one shared limits constant.
No JSON grammar or tree decoder was added. Expiry, pressure eviction, bounded
record/context lookup and pin-protection capture use this stored-row owner;
aggregate query capture retains its existing opaque-row charge, entry ceilings,
lease and cancellation rules. The default pass limits remain 32 MiB catalog,
32 retirement units and 4,096 payload references. Rows whose actual shape still
exceeds admission refuse; this does not make every configured artifact size
retirable under every smaller pass budget. Larger manifest paging remains a
separate capacity concern.

The regression retains all original bytes during live traversal, performs bounded
pressure eviction, then expires the remaining unprotected records while preserving
the record pin and its exact bytes. Releasing that pin permits complete logical
payload reclamation. Dense input still refuses under a finite allowance; malformed
native JSON reports storage failure without spending that allowance. These checks
cover logical ownership and cursor progress, not physical compaction or sustained
production retention throughput.

Signed source `081f981` passes the full gate: formatting, release Clippy with
warnings denied, 156 storage scenarios, 13 core unit tests, 60 RPC integration
scenarios, seven RPC and three daemon unit tests, remaining workspace tests,
seeded census tests and the pinned cross gate (244 ledger jobs, zero distinct
exceptions). The real-daemon 8,000-record direct/MCP cancellation scenario also
passes independently in 7.21 seconds, without the earlier retention refusal.
No schema, storage representation, dependency or PurRDF revision changed.

All five selected executables were rebuilt and frozen with source and SHA-256
evidence at `~/.local/state/sparrow/receiver-history-images-q4w_vn4s`. Private
qualification `~/.local/state/sparrow/typed-load-qualification-xus6nm2s` runs the
existing typed load client against that exact daemon: 32 mixed-format cohorts,
4,160 records and 8,678,582 original bytes, complete query traversal, exact-byte
checks and released captures. It completes in 8.058163 seconds, with sampled
high-water RSS 183,844 KiB, closed native files totaling 16,687,452 bytes, and
clean SIGINT teardown in an observed 0.114 seconds. The public unit remains
unchanged during private qualification. These observations establish neither a
production throughput target nor a worst-case memory/shutdown bound.

Controlled adoption evidence is `/tmp/sparrow-expiry-live.204gpmxe`, including a
closed-store backup. The enabled active unit runs PID 1703194 from
`sparrowd-schema48-081f981`; the stable CLI resolves to
`sparrow-schema48-081f981`. Frozen, installed and alias hashes agree, the daemon
retains `cap_net_bind_service=ep`, and its listener is `0.0.0.0:518`.
TCP/Unix/CLI/MCP checks preserve store
`store:46c88e949ccec658600b66c7becd5bb7`, format 37, 212 records, healthy watches,
zero pins and read leases. Saved intents preserve their original receipts, bytes,
clocks and replacement files without new intake or retirement. The complete
21-source inventory, five generation checkpoints, grouped pages, temporal/field
witnesses, native text scores, four JSON keys and twenty values remain exact.
Syslog listeners remain unconfigured. Complete governed SPARQL, correlation,
larger-manifest capacity, physical reclamation, sustained resource/latency and
reboot acceptance remain unfinished.

## Final RPC admission drain

Signed source `c7e3960` joins owned intake workers, then reserves the complete
native RPC operation budget before the final WAL flush. Tokio's fair semaphore
reservation prevents fresh work from overtaking that drain. Each admitted
blocking worker retains its permit through completion even if its caller
disappears. Once the reservation succeeds, admission closes and its guard stays
held through flush and endpoint cleanup. Closed dispatch returns retryable
`SHUTTING_DOWN`; callers retry the same intent after reconnecting. Intake joins
retain RPC availability for their own unfinished imports. If draining fails,
teardown reports the failure and withholds the final flush rather than presenting
a fence over potentially active writes. No schema, storage format, dependency,
or PurRDF revision changed.

The signed source commit's full gate passes formatting, release Clippy with
warnings denied, 155 storage tests, 60 RPC integration tests, seven RPC unit
tests, three daemon unit tests, all remaining workspace tests, seeded census
tests, and the pinned cross gate (243 ledger jobs, zero distinct exceptions).
Two new daemon tests exercise fair full-budget reservation and the actual
teardown owner. The latter cancels an admitted worker's caller, releases its
paused buffered write, checks the final durable watermark, and reopens the store
for exact byte readback. The RPC cancellation test also checks that closed
admission rejects a real mutation without publication and renders a retryable
typed failure.

All five selected images were explicitly rebuilt after the gate and frozen at
`~/.local/state/sparrow/receiver-history-images-x2lfe272`, with complete source
identity and SHA-256 evidence. Private qualification
`~/.local/state/sparrow/rpc-drain-traffic-ctqm4kiw` sends SIGINT during the actual
typed load workload after observing a nonempty native anonymous staging file
below its owned store. It distinguishes that file from deleted table files by
the Linux temporary-file name. Shutdown exits zero in an observed 0.214 seconds
and removes the owned endpoint. The interrupted workload reports a transport
failure and emits no successful qualification report. Reopening preserves its
store identity and 194 retained records; SDK reads match every byte of all five
accepted artifacts against their saved inputs. Final pending writes/bytes,
read handles and pins are zero. The public unit remains unchanged throughout
this private probe. This is a bounded observation, not a worst-case shutdown
deadline or a claim that every request sent before interruption was admitted.

Public adoption evidence is `/tmp/sparrow-drain-live.ac9jj07r`, including a
closed-store backup before switching aliases. The enabled active unit runs
PID 1518105 from `sparrowd-schema48-c7e3960`; the stable CLI resolves to
`sparrow-schema48-c7e3960`. Frozen, installed and alias hashes match, the daemon
has `cap_net_bind_service=ep`, and its listener is `0.0.0.0:518`.
TCP/Unix/CLI/MCP checks preserve store
`store:46c88e949ccec658600b66c7becd5bb7`, format 37, 212 records, healthy watches,
zero pins and read leases. Saved intents preserve receipts, bytes, clocks and
replacement files without new intake or retirement. All 21 sources, five
generation checkpoints, grouped pages, temporal/field witnesses, native text
scores, four JSON keys and twenty values remain exact. Syslog listeners remain
unconfigured. Complete governed SPARQL, correlation, sustained resource/latency
acceptance and reboot qualification remain unfinished.

## Persistent interrupt subscription

Signed source `b8a9a6c` registers one native SIGINT subscription before storage
opening and worker startup, retaining it through awaited maintenance branches.
The previous serving loop recreated `ctrl_c()` inside each `select!` iteration;
selecting another branch dropped that subscription while its worker was awaited.
Tokio 1.53.2 implements `ctrl_c()` by creating a fresh native signal receiver,
and each registry subscription starts at the current watch version. Retaining
the receiver removes this lost-notification window and routes early interrupts
through the existing complete teardown owner. Individual native calls remain
non-interruptible. No schema, format, runtime dependency or PurRDF pin changed.

The full source gate passes formatting, release Clippy with warnings denied,
155 storage tests, 60 RPC integration tests, all remaining workspace tests,
seeded census tests and the pinned cross gate: 243 ledger jobs and zero distinct
exceptions. The new real-process regression sends SIGINT after the TCP startup
announcement, before native store/worker initialization, then verifies exit zero,
owned endpoint cleanup, reopening and exact SDK byte readback. Its initial
failure incorrectly expected the fixture's connect address in the announcement;
the assertion now checks its actual configured `0.0.0.0` binding.

All five selected executables were built after the gate and frozen with exact
source and SHA-256 evidence at
`~/.local/state/sparrow/receiver-history-images-ki7wt_cg`. Private comparison
`interrupt-startup-stwx4aop` signals old and new images during actual startup:
all three old-image trials terminate under SIGINT, while all three new-image
trials finish initialization and teardown with exit zero and no socket. The
new-image observations range from 0.915 to 1.166 seconds. Earlier development
probe `interrupt-startup-kqcqc2ns` refused its incomplete configuration; corrected
development comparison `interrupt-startup-415ialb3` showed the same behavior.

Two frozen private 128-cohort load attempts, `typed-load-qualification-4zk0g50_`
and `typed-load-qualification-mw88zyrx`, preserve the expected 10,000-match
`RESOURCE_LIMIT`. Their workload tools fail explicitly and emit no successful
report. Subsequent SIGINT teardown exits zero after observed waits of 0.114
seconds, removes both owned sockets and preserves the public unit throughout
qualification. Their source/images, configuration, inputs, RSS/I/O samples,
failure stderr, shutdown observations and closed files remain in those
directories. These probes address the concrete subscription defect; the
original untraced timeout's exact cause and a worst-case shutdown latency bound
are not established retroactively.

Controlled public adoption evidence is `/tmp/sparrow-interrupt-live.c94zb7e4`.
The enabled active unit runs PID 1407227 from `sparrowd-schema48-b8a9a6c`; the
stable CLI resolves to `sparrow-schema48-b8a9a6c`. Frozen/installed/alias hashes
match, the daemon retains `cap_net_bind_service=ep`, and the observed listener
is `0.0.0.0:518`. TCP/Unix/CLI/MCP readback preserves
`store:46c88e949ccec658600b66c7becd5bb7`, format 37, all 212 records, zero
pins/read leases and healthy watches. Saved intents retain their original
receipts, bytes, clocks and replacement files without new intake or retirement.
The 21-source inventory, five generation checkpoints, grouped pages, temporal
and field witnesses, native text scores, four JSON keys and twenty values remain
exact. Syslog listeners remain unconfigured. Reboot, sustained resource/latency
acceptance, complete governed SPARQL and correlation remain unfinished.

## Concurrent typed load qualifier

Signed source `6900752` adds a dev-only `load` executable that coordinates
durable SDK ingestion and complete agent capture traversal through production
renderers. [Its report](load-qualification.md#qualified-runs) records frozen
images, exact source bytes, native count checks, latency observations, sampled
daemon RSS and closed-store growth for 32 and 64 mixed-format cohorts. The
larger successful run accepts 17,358,454 bytes and 8,320 records, with 28
concurrent captures and zero leaked handles/bytes. The full source gate passes
155 storage tests, 59 RPC tests, all remaining workspace tests and 243 helper
ownership jobs with zero distinct exceptions.

Attempts beyond the existing 10,000-row broad capture limit refuse explicitly.
One failed-load shutdown exceeded its observation bound; subsequent reopen and
repeat probes exit cleanly, but no causal fix or shutdown acceptance is claimed.
The qualifier does not establish sustained production volume or full latency/
resource guarantees. The installed public source, PID, store and configuration
remain unchanged; no service adoption was needed for this dev-only executable.

## Catalogue capture cancellation

Signed source `8b56d65` extends the existing connection/caller cancellation
owner to source, artifact, pin-list and pin-member captures. They use the same
native catalogue admission and publication owner as ordinary reads. Checkpoints
cover metadata scans and final publication; cancelled fresh captures are not
retained. The generic RPC reply guard distinguishes fresh captures from
continuations: abandoning a continuation preserves the previously delivered
snapshot and original cursor. Immutable store identity is validated at open and
can be read without acquiring the catalogue writer, keeping identity validation
off the event loop's storage wait path. Admitted mutations retain their delivery
semantics. No schema, format, runtime dependency or PurRDF pin changed.

The full source gate passed formatting, release Clippy with warnings denied,
155 storage tests, 59 RPC integration tests, all remaining workspace tests,
seeded census tests and the pinned cross gate: 242 ledger jobs and zero distinct
exceptions. Native scenarios cover cancelled fresh and continued metadata reads,
original cursor replay, cancellation during initial page staging, and unchanged
records/pins/bytes. One shared RPC unit scenario exercises records, sources,
artifacts, pin lists and direct members under caller teardown and connection
closure before and after capture, preserving worker admission until exit.
An admitted mutation still publishes after caller cancellation. An initial census
run rejected an ownership entry for a unit-test-only helper; that entry was
removed because the existing scanner excludes `cfg(test)` modules, while the
scenario remains a verification vector of the production cancellation owner.
No detector or gate was bypassed.

All five selected executables were built after the gate and frozen with exact
source and SHA-256 evidence at
`~/.local/state/sparrow/receiver-history-images-ix_dl4_q`. Private closed Btrfs
copies `catalog-cancel-unix-4c8qee54` and `catalog-cancel-tcp-iq_nrzc2` each
interrupt twelve actual record captures after observing partial anonymous
staging. Cleanup is observed within the private two-second bound, with zero
record handles/bytes. All 7,058 records match the full 48-page baseline after
fresh traversal and reopening; the public service is unchanged during these
runs. These transport tests qualify the shared record reply/publication path;
metadata lifetime races are covered by the deterministic native/RPC scenarios,
not by a claimed metadata transport latency measurement.

Controlled public adoption evidence is `/tmp/sparrow-catalog-cancel-live.bxkly9bs`,
including frozen/installed hashes, complete source gate output, private drivers,
closed pre-adoption backup and readback reports. The enabled active unit runs
PID 1228283 from `sparrowd-schema48-8b56d65`; the stable CLI resolves to
`sparrow-schema48-8b56d65`. The daemon retains `cap_net_bind_service=ep` and
the observed `0.0.0.0:518` listener. TCP, Unix, CLI and MCP readback preserve
`store:46c88e949ccec658600b66c7becd5bb7`, format 37, all 212 records, zero
pins/read leases and healthy watches. Saved intents retain exact original
receipts, bytes, clocks and replacement files without new intake or retirement.
The 21-source inventory, five generation checkpoints, grouped pages, temporal
and field witnesses, text scores, four JSON keys and twenty values remain exact.
Syslog listeners remain unconfigured.

Individual native calls, sorts and storage mutex waits are not interruptible;
their next host checkpoint observes cancellation. Direct Cap'n Proto promise
cancellation, sustained cancellation resource/latency bounds, reboot acceptance,
complete governed SPARQL and correlation remain unfinished.

## Durable receiver history

Signed source `32b288a` adds finite durable syslog receiver windows, immutable
per-record receiver positions, sorted source references and typed
`receiverWindowGet`. CLI `receiver-window STORE_ID WINDOW_ID` and the closed
read-only MCP `receiver_window_get` share one generated lookup and renderer.
Full record/context metadata exposes positions; use CLI `context STORE_ID
RECORD_ID --before 0 --after 0` to resolve a search result's occurrence metadata.
Positions identify durable observation commit order, never sender or network
arrival order. Accepted bytes and positions publish in one synchronized WAL
batch. Known local refusals, UDP truncation, TCP framing failures and frame
deadlines publish classified observations without raw packet occurrences.
Sender/network/kernel and pre-commit loss remain explicitly unknown. Failed
history publication stops intake with a visible failure rather than reporting
a durable counter update. Rejected connections do not invent message positions.

Producer and receiver windows share bounded native registry decoding, inclusive
gap arithmetic and retained-window lookup. Their semantics remain separate.
The registry admits 64 receiver windows and 128 KiB, retaining 16 recent discard
intervals while preserving exact aggregate counts. Summaries expire 30 days
after their latest observed timestamp; record pins never renew them. Listener
restart and post-expiry reuse create new window identities. A first receiver
publication fences a store at format 40, or 41 with watch state; later writes
preserve that fence and pressure state. Production has no syslog listeners or
receiver observations and retains format 37.

The source gate passed formatting, release Clippy with warnings denied, 154
storage tests, 59 RPC tests, all remaining workspace tests, seeded census tests
and the pinned cross gate: 242 ledger jobs and zero distinct exceptions. Tests
cover concurrent unique commit positions, exact raw bytes and independent
occurrences, native RDF receiver metadata, discard totals and interval trimming,
restart/reopen, pinned bytes after summary expiry, watch-fence preservation,
bounded registry admission, duplicate identities, missing/fence-mismatched and
oversized registry failures. Real TCP/UDP integration verifies typed store/schema
refusals and canonical CLI/MCP replies. Initial gate failures identified an
unnecessary test clone and duplicate lookup logic; both were corrected, including
shared locking/ID/expiry ownership. No gate or detector was bypassed.

All five selected executables were built after the gate and frozen with exact
source identity and SHA-256 hashes under
`~/.local/state/sparrow/receiver-history-images-8kqfpif3`. Private qualification
`receiver-history-qual-gl0iavii` uses an owned Btrfs store and actual loopback
TCP/UDP ingestion. Two complete TCP packets, one framing failure and one frame
timeout produce four TCP observations; two UDP packets, including malformed
protocol input, and one oversized datagram produce three UDP observations.
Frozen empty source continuation omits later windows while fresh inventory
discovers them. Context readback certifies each immutable position and exact
bytes. Reopening preserves summaries and positions, a new listener instance
creates a third window at ordinal one, and removing bindings retains all five
accepted records and historical references. The previous schema 47 daemon
refuses format 41. Clean shutdowns complete and the public unit remains unchanged
during qualification. An earlier driver run `receiver-history-qual-1jai15h5`
stopped because it incorrectly expected full occurrence metadata in projected
search rows; the final driver reads it through the existing context operation.

Controlled public adoption evidence is
`/tmp/sparrow-receiver-history-live.cn4rlmk7`. The enabled active user unit runs
PID 978606 from `sparrowd-schema48-32b288a`; stable CLI
`~/.local/bin/sparrow` resolves to `sparrow-schema48-32b288a`. Installed and alias
hashes match the frozen images, with `cap_net_bind_service=ep` on the daemon and
an observed `0.0.0.0:518` listener. Native LAN TCP and Unix readback preserve
`store:46c88e949ccec658600b66c7becd5bb7`, format 37, 212 records, zero pins/read
leases and healthy watch state. Saved intents preserve original receipts, exact
bytes, clocks and replacement files with no new intake or retirement. All 21
sources preserve their previous fields, with empty receiver/listener references;
five file-generation checkpoints, grouped queries, temporal/field witnesses,
native text scores, four JSON keys and twenty values remain exact. MCP declares
33 closed tools, including read-only receiver lookup with typed `NOT_FOUND`.
Syslog configuration, runtime dependencies and the PurRDF pin are unchanged.

This qualifies finite receiver observation/history behavior. Sender continuity,
production syslog bursts, worst-case discard admission latency, external network
loss, power loss and reboot acceptance are not established by these scenarios.
Complete governed SPARQL admission/egress and correlation remain unfinished.

## MCP notification cancellation

Signed source `5bdc775` retains each cancelled MCP job's admission and request-ID
slot until that job is actually retired. Notifications request abort without
removing the pending slot early; both successful and aborted completions retire
the slot through one owner. Thus outstanding jobs and pending IDs remain bounded
by four during cancellation bursts. Aborted jobs emit no tool reply. Each tool
already owns its own SDK connection; dropping it closes that transport and reaches
the daemon's connection-scoped cancellation introduced by `d4b9aaf`. The stdio
session and sibling tool connections remain available. No schema, store format,
runtime dependency or PurRDF pin changed.

The real-daemon scenario imports 8,000 occurrences of one native JSONL payload,
observes anonymous capture allocation and sends actual notifications for record,
value, text and key queries over Unix and TCP, using numeric and string IDs.
Cancelled allocation disappears; ping/status continue in the same initialized
stdio session, with zero query leases/bytes and unchanged records. A subsequent
complete MCP traversal returns every original intake ID exactly once and releases
its capture. The fixture's initial 128 MiB allowance refused corpus preparation
with `STORAGE_PRESSURE`; its final private 1 GiB allowance uses the same storage
guard and leaves production configuration unchanged. The full source gate passed
151 storage tests, 59 RPC tests, all remaining workspace tests, seeded census tests
and the pinned cross gate: 236 ledger jobs and zero distinct exceptions.

Selected executables were built after that gate and frozen with exact source and
SHA-256 evidence at `~/.local/state/sparrow/mcp-cancel-images-q3hhwgtj`. Independent
closed 7,058-record Btrfs copies at `mcp-cancel-unix-l9gui3d9` and
`mcp-cancel-tcp-yyl4fm_2` each perform twelve notification cancellations during
observed partially written captures. The MCP process stays alive; each cancelled
tool produces no reply while its subsequent ping and status succeed. No status
or result-release call runs during allocation cleanup observation. Maximum cleanup
upper observations were 1.263 ms for Unix and 1.288 ms for TCP, within the private
two-second qualification bound. These include notification transmission and
sampling; they are not exact checkpoint timestamps or a production guarantee.
Every trial ends with zero read leases/bytes and pins. Fresh and reopened captures
preserve the initial 7,058 rows over 48 pages and store identity, with clean closes.
Both private runs leave the public owner unchanged.

Public adoption evidence at `/tmp/sparrow-mcp-cancel-live.gz0m0r9k` retains the
closed predecessor backup, saved-intent retries, full source/generation inventory,
grouped/JSON/field/time/text comparisons, tool declarations/refusals, image/alias
hashes and unit/listener readback. The enabled unit is active at PID 784520 on
`0.0.0.0:518`, LAN `192.168.2.4:518`, and the existing Unix socket. Store
`store:46c88e949ccec658600b66c7becd5bb7`, format 37 and 212 records are unchanged,
with zero pins/read leases and healthy watches/durability. No new public intake
or retirement was performed.

This qualifies explicit MCP notification propagation for the four capture
families and their bounded adapter job lifetime. Direct Cap'n Proto promise
cancellation, other read families, sustained production cancellation bursts and
worst-case native-call/mutex/sort latency remain unfinished. Complete governed
SPARQL/correlation/explanations, durable receiver windows, production volume/quota
qualification, forced physical reclamation and reboot acceptance also remain
unfinished.

## Cooperative capture cancellation

Signed source `d4b9aaf` binds record/text/key/value capture workers to independent
RPC and production connection lifetimes. The shared native capture owner checks
that lifetime during candidate, fact, posting, corpus/scoring and staging work.
Disconnection, failure, connection lease expiry or serving task teardown signals
children without changing mutation delivery behavior. Concurrency admission stays
with the blocking worker until it exits. A reply guard releases completed captures
abandoned before rendering. Successfully rendered cursors retain their ordinary
lease for retry/reconnect. No schema, store format, PurRDF pin or dependency changed.

The worker scenario covers request cancellation and explicit connection teardown
both before capture and after publication, abandoned returned replies, successful
reply ownership and preservation of admitted mutation work. The complete source
gate passed 151 storage tests, 58 RPC tests, the remaining workspace tests, seeded
census tests and the pinned cross gate: 234 ledger jobs, zero distinct exceptions.

Selected executables were rebuilt after the gate and frozen with exact source
and image hashes at `~/.local/state/sparrow/query-cancel-images-5v_tgs7_`. Separate
closed 7,058-record Btrfs copies at `query-cancel-unix-jo5yhpwe` and
`query-cancel-tcp-5rxj6oiy` each interrupt twelve owned clients during observed
partially written anonymous captures. No status or explicit release call runs
during cleanup observation. Every staging file disappears within the private
two-second qualification bound; maximum sampled cleanup upper observations were
1.165 ms for Unix and 1.216 ms for TCP. These include client kill/wait and sampling,
not an exact server checkpoint timestamp or production latency guarantee. Each
subsequent status has zero read handles/bytes and pins. Fresh and reopened captures
preserve all 7,058 initial rows over 48 pages, store identity and clean closure;
the public owner is unchanged during both private runs.

An earlier RPC-only cancellation candidate passed its local race scenario but
failed actual disconnect qualification at `query-cancel-unix-benzoewj`: this
Cap'n Proto path retained the server call after peer exit. The final implementation
adds explicit connection ownership, rather than assuming that dropping a remote
promise stops the blocking worker. Earlier corrected uncommitted-image runs at
`query-cancel-unix-4pn_57ek` and `query-cancel-tcp-41e60ukv` also passed; the final
qualification above uses the signed source's separately rebuilt frozen images.

Public adoption evidence at `/tmp/sparrow-query-cancel-live.g_f6wz4r` contains the
closed predecessor backup, saved-intent retries, complete source/generation
inventory, grouped and JSON queries, field/time witnesses, native text scores,
MCP declarations/refusals, installed image/alias hashes and unit/listener readback.
The enabled unit is active at PID 708683 on `0.0.0.0:518`, LAN
`192.168.2.4:518`, and the existing Unix socket. Store
`store:46c88e949ccec658600b66c7becd5bb7`, format 37 and 212 records are unchanged,
with zero pins/read leases and healthy watches/durability. No new public intake
or retirement was performed.

Individual native calls, sorts and mutex waits finish before the next host
checkpoint. Explicit cancellation-message propagation through every adapter,
production tail-latency/resource bounds and sustained burst qualification remain
unfinished. Complete governed SPARQL/correlation/explanations, durable receiver
windows, production volume/quota qualification, forced physical reclamation and
reboot acceptance also remain unfinished.

## Stored RDF graph layers

Signed source `40b0e6c` adds native `RdfGraphLayers` selection through
`Store::with_rdf_record_layers`. Metadata, occurrence diagnostics and shared
content can be selected independently through the existing bounded immutable
RocksDB view. `with_rdf_records` delegates with all layers; no second reader,
projector or evaluator was added. Selecting diagnostics alone does not inspect
an excluded content projection's failed or corrupt state. Record identity,
pin-aware eligibility and read admission still apply, including empty layer
selection. Wire schema, store format, runtime dependencies and PurRDF pin are
unchanged.

The real-store scenario exercises all eight combinations, deduplicated content,
native governed diagnostic queries and metadata/diagnostic joins, all-layer
facade equivalence, failed and corrupt content state, request refusal and
pin/release/expiry boundaries. The full source gate passed 151 storage tests,
58 RPC tests, the remaining workspace tests, seeded census self-tests and the
pinned cross-workspace check: 233 ledger jobs and zero distinct exceptions.

Selected executables were built after that gate and frozen with exact source
and SHA-256 evidence at `~/.local/state/sparrow/rdf-layers-images-kn4ilfus`.
Public adoption evidence at `/tmp/sparrow-rdf-layers-live.7obb8b59` retains the
closed predecessor backup, installed image/alias hashes, saved-intent retries,
complete source inventory and generation traversal, JSON keys/values, grouped
queries, field/time witnesses, native text scores, MCP declarations and typed
refusals. The first source readback overlapped a JSON readback and failed its
global zero-lease assertion; a sequential rerun passed. Final readback confirms
zero leases, healthy watches/durability and unchanged store identity
`store:46c88e949ccec658600b66c7becd5bb7`, format 37 and 212 records. The enabled
unit is active at PID 664147 on `0.0.0.0:518`, LAN `192.168.2.4:518`, and the
existing Unix socket. No new public intake or retirement was performed.

This qualifies a native integration seam and the existing public query paths.
It does not add a public SPARQL/graph-layer RPC, CLI or MCP operation. Complete
native query admission, diagnostic coverage/correlation/explanation contracts,
durable syslog receiver windows, prompt query cancellation, production
volume/quota qualification, forced physical reclamation and reboot acceptance
remain unfinished.

## Syslog listener discovery

Signed source `cf16519` adds schema 47 source references and the generated
`syslogListenerGet` operation; subsequently adopted source `f9fee6c` also preserves
typed registry failures on source listing. Source pages now carry sorted
`syslog_listener_ids` supplied by the runtime registry, including listeners with
no packets and several listeners sharing one source. Configured IDs and exact
source labels count against the existing scan/capture admission. Continuation
freezes the original bindings. No new parser, scoring law or runtime dependency
is introduced, and no store format migration is needed.

`syslog-listener STORE_ID SOURCE LISTENER_ID` and the closed read-only MCP tool
`syslog_listener_get` use that same typed RPC. Store/source/schema/ID guards
refuse wrong namespaces, old schemas, invalid declarations and missing or rebound
listeners. The reply exposes the existing transport/framing, actual binding,
bounds, active connections and observed counters. Status and both adapters use
one canonical listener renderer. Counters are current process observations and
reset on restart; producer continuity and UDP kernel/network loss remain unknown.

The native storage scenario qualifies shared-source sorting, empty listeners,
frozen continuation after binding removal, exact source selection, duplicate and
oversized declaration refusal, tiny scan refusal without a lease, and reopen
without persisting runtime authority. The existing real daemon TCP/UDP packet
scenario now traverses source pages from TCP CLI through Unix MCP, resolves every
empty listener through generated Rust, CLI and MCP, checks namespace/schema
failures and compares counters after real intake. A separate actual poisoned
registry scenario verifies typed `STORAGE_FAILURE` from both lookup and source
discovery, with no source lease published. Its daemon configuration was moved
into `sparrow-test-support` for one dev-only implementation shared with RPC tests.
The first correction gate rejected a test function listed as a helper entry point;
the row now names the actual shared fixture home and retains both test files as
vectors. No detector or duplicate exception was changed. The final full source
gate passed: 150 storage scenarios, 58 RPC scenarios, the remaining workspace
tests, seeded census self-tests and 233 ledger jobs with zero distinct exceptions.

The selected executables were built after verification and frozen with source and
SHA-256 readback at `~/.local/state/sparrow/syslog-discovery-images-mzod7gq1`.
Independent Btrfs qualification at `syslog-discovery-qual-2gz_ct5d` uses three
private loopback listeners, two sharing a source. Before intake, all bindings are
discoverable with zero records. Two identical modern TCP packets, the same UDP
packet and a malformed UDP packet create four independent records; an oversized
datagram is observed as truncated without publication. Listener counters match
those observations. A source capture taken before intake still returns its
original zero count after publication, while a fresh read reports four records.
All four byte exports match their original inputs, including three equal payloads
with distinct occurrences. Reopen resets listener counters while preserving all
four IDs and exact bytes. Removing the shared listeners removes only their fresh
bindings: their retained source still reports four records, and lookup refuses
with `NOT_FOUND`. Final read handles are zero and the private owner closes cleanly.

The earlier `syslog-discovery-qual-nzxz4u67` driver stopped on incorrect export
field names; the corrected qualification uses the actual `byte_start`/`byte_end`
contract. Parent image `cf16519` also passed at `syslog-discovery-qual-x_9mxfdu`.
Its independent closed 7,058-record copy at `json-pages-qualification-5ot9qbge`
preserves predecessor record/text rows, ranking witnesses, artifact groups,
51 keys and 286 value occurrences. Broad traversal returns every record in
48 bounded pages. Fingerprinted images, drivers, logs, original/exported bytes
and reports retain these methods; private runs leave the public owner unchanged.

Final public adoption evidence is
`/tmp/sparrow-syslog-discovery-live.o2sdh4yt`, with the closed predecessor backup,
saved-intent retries, source/generation pages, grouped/JSON/field-time comparisons,
tool declaration, typed missing-listener replies, image/alias SHA readback,
capability, listener and unit evidence. The enabled unit is active at PID 593030
on `0.0.0.0:518`, LAN `192.168.2.4:518`, and the existing Unix endpoint. All
21 sources preserve every predecessor field; the added listener references are
explicitly empty because public syslog remains unconfigured. Store identity
`store:46c88e949ccec658600b66c7becd5bb7`, format 37 and 212 records are unchanged,
with zero pins/read leases and healthy watches/durability. No new public intake
or source retirement was performed; the saved-intent driver's historical
two-record checkpoint wording describes its original acceptance, not this cutover.

This completes current listener discovery, not durable receiver-stream windows
or per-record receiver sequence/gap relationships. Those contracts, complete
governed SPARQL/correlation/explanations, prompt query cancellation, production
volume/quota qualification, forced native physical reclamation and reboot
acceptance remain unfinished.

## Abandoned query capture cleanup

The frozen uncapped `ebea75a` images were separately qualified on two private
Btrfs copies of the closed 7,058-record corpus:
`~/.local/state/sparrow/query-disconnect-c2cyovfv` for Unix and
`~/.local/state/sparrow/query-disconnect-tcp-upkug15y` for native TCP. Each run
first traverses all 48 broad-query pages and explicitly releases that baseline.
It then starts three query clients sequentially and kills only each owned client
after observing a nonempty, partially written anonymous capture through the
private daemon's file descriptors. These are actual in-progress captures, not
idle connections or completed replies. No public client or daemon is killed.

Each complete capture has 2,442,774 data bytes. Unix interruptions observe
27,288, 29,362 and 16,918 bytes; TCP interruptions observe 20,026, 17,610 and
23,831 bytes. Every interrupted client has empty stdout. The admitted storage
work nevertheless finishes, leaving one, two, then three result leases and
complete anonymous files of 2,445,312 allocated bytes each. A subsequent status
response arrives 60–65 ms after each client kill. That duration includes CLI
startup and status execution; it is not a direct worker-cancellation latency.

After those three observations, the driver makes no status or release call
while sampling anonymous file descriptors every 100 ms. The owner alone purges
the abandoned staging. All files are observed gone 60.78–60.91 seconds after
the Unix interruptions and 60.70–60.83 seconds after the TCP interruptions.
These are sampled upper observations for the last file's removal, not precise
per-file expiry timestamps. Final status reports zero read handles/bytes and
unchanged 7,058 records with no pins. Complete fresh traversals after expiry and
after a graceful close/reopen exactly equal the original 7,058 captured rows in
all 48 pages. Store identity is preserved and both owned endpoints are cleaned.
The public unit remains active at PID 406435 throughout both runs.

Each fixture retains `driver.py`, image/source fingerprints, client output,
daemon logs, all 606/607 maintenance observations and statuses in `report.json`,
and a fingerprinted `qualification-summary.json`. The earlier
`query-disconnect-tuau0m57` attempt stopped on an invalid CLI argument before
query execution; the successful drivers use the actual records CLI contract.
This qualifies bounded abandoned-result cleanup at the existing lease deadline.
Disconnect does not promptly cancel the admitted capture or reclaim its lease.
Prompt in-flight cancellation, its resource/latency bound, and sustained
abandonment under production offered load remain unfinished acceptance work.

## Btrfs quota qualification prerequisite

The isolated quota attempt at
`~/.local/state/sparrow/btrfs-quota-ta5r2obw` did not run a quota workload.
It created a new sparse 512 MiB image, formatted only that owned file as Btrfs,
and attempted its mount inside a private root mount namespace. Mount returned
32: it could not set up a loop device. No loop block device was available in the
readback, and `modinfo loop` reported no available module. The host `/home`
filesystem separately reported quotas disabled. Neither its quota configuration
nor kernel/module configuration was changed to make the experiment pass.

The failure occurred before quota enable, subvolume creation or daemon startup.
`supervisor.json`, `supervisor-trace.txt`, `mount-failure.txt` and
`host-prerequisites.json` retain the attempted method and prerequisite evidence.
After namespace exit, the owned image has no loop association, the outer mount
directory is empty and still resolves to the original host filesystem, and the
public service remains active at PID 406435. This is an unrun Btrfs quota test,
not quota-error or recovery qualification. The real tmpfs ENOSPC result below
remains separate evidence. A quota-capable isolated filesystem is still needed
for this acceptance case; enabling host quotas or changing the host kernel is
outside this experiment.

## Filesystem-full recovery

The currently installed source `ebea75a`, schema 46, has a separate real
filesystem-full qualification at
`~/.local/state/sparrow/fs-full-qual-3u16g3kc`. It uses the frozen uncapped images
from `tail-discovery-images-5du4trjg`, with SHA-256 readback, inside a private user
and mount namespace. A 64 MiB tmpfs contains only an owned test store and ballast;
the application budget is 1 GiB with 128 MiB reserve. This deliberately exercises
native filesystem failure independently of application admission. No host mount,
quota, producer, public service configuration or installed image is changed.

Two log records are durably accepted and protected by one seven-day pin. Writing
53,616,640 ballast bytes outside the data directory reaches errno 28, `ENOSPC`;
`statvfs` reports zero available bytes. One subsequent 2 MiB opaque trial receives
a durable receipt and one independent record. A second distinct 2 MiB trial
refuses with `STORAGE_FAILURE`, empty stdout and a native WAL append `No space
left on device` error. The run accounts for confirmed receipts rather than
assuming every write must fail immediately from a free-space observation.

Status remains readable with three confirmed records, the original pin and an
explicit durability failure. Both pinned record byte spans remain exact while
the filesystem is full. Application pressure remains ready with zero retirement
passes because its tracked files are below the application budget. Removing the
ballast restores filesystem capacity; the existing process still reports its
durability failure on final flush and exits with code 1, while cleaning its owned
socket. This is an explicit failure, not a successful graceful-close claim.

Reopen preserves all three confirmed records, the pin, seed receipt and original
pin mutation receipt. The refused import resumes through its durable journal,
accepts exactly one new record, and returns the same receipt on retry. All seed
and accepted trial bytes are checked within the one-MiB range ceiling. Pin release
works after recovery. Two subsequent close/reopen boundaries succeed with four
records and no pins. The final closed store is copied out before namespace exit;
that Btrfs copy independently reopens with matching store identity, all original
durable receipts, exact bytes and healthy durability. The namespace mount is gone
and the public unit remains PID 406435 throughout.

`report.json`, `driver.py`, input fingerprints, daemon/restore logs, export chunks,
`restored-report.json`, mount evidence and before/after public unit evidence retain
the method. An earlier fixture at `fs-full-qual-tlrpt13a` stopped because its count
assertion omitted an acknowledged opaque record; the final driver counts each
confirmed receipt and respects artifact/pin IDs and bounded byte reads. This
qualifies one real ENOSPC/native WAL failure and process recovery. It does not
establish Btrfs quota behavior, hardware or power-loss recovery, sustained volume,
forced native physical reclamation, or complete operational acceptance.

## Tail generation discovery

Signed source `ebea75a` adds sorted `tail_generation_ids` to the existing source
capture. Active and unexpired terminal checkpoints are grouped by their exact
intake key, including empty generations with zero records. The native tail
registry decoder remains the identity/validation owner. Registry bytes and
entries use the shared catalog scan admission; references use fallible exact
reservation and actual source capture/lease capacity accounting. Missing or
corrupt required metadata refuses rather than disappearing from discovery.
Continuation freezes the original IDs, without retaining bytes or extending
checkpoint metadata lifetime. Fresh reads exclude terminal rows at their exact
metadata deadline. Existing typed checkpoint lookup supplies offsets, original
declarations, completion and gaps.

Wire schema 46 adds `SourceSummary.tailGenerationIds @10`; source query schema
validation and adapter discovery refuse old queries. CLI and MCP share the
canonical renderer. Storage format is unchanged. The full release gate and
normal signed source hook pass 149 storage scenarios, 58 RPC scenarios, all
remaining workspace tests, seeded census self-tests and 229 pinned ownership
jobs with no distinct exceptions. New/expanded scenarios cover empty inputs,
terminal expiry, a generation added after capture, exact source selection,
restart, scan refusal, corrupt registry refusal and typed/TCP/CLI/MCP discovery
through source continuations. Five source storage scenarios and the expanded
generation RPC scenario also pass on Btrfs; source and logs are retained at
`~/.local/state/sparrow/tail-discovery-qualification-crgemz5t`.

The selected runtime build follows the source hook. Frozen images are
`~/.local/state/sparrow/tail-discovery-images-5du4trjg`. The closed copied corpus
at `~/.local/state/sparrow/json-pages-qualification-cuykzn6s` preserves all 7,058
record rows, source/temporal/text results, native scores, groups, 51 keys and
286 value occurrences against predecessor evidence, with zero result leases.

Public adoption evidence is `/tmp/sparrow-tail-discovery-live.aoq9whkm`. The
predecessor closes gracefully before a closed-store backup and immutable alias
replacement. The enabled unit now runs PID 406435 on `0.0.0.0:518` and the
existing Unix socket, with schema 46, format 37 and the same store identity and
212 records. All five development generations resolve through source discovery
and exact checkpoint lookup over TCP/Unix/CLI/MCP. The complete 21-source
inventory matches across continuations. The initial live driver incorrectly
treated its default byte-bounded 15-row page as the whole baseline; the corrected
driver traverses and releases every capture. Old schema-45 source queries refuse.
Saved finish/handoff retries, grouped and JSON pagination, native field/time/text
witnesses and idle-peer admission also pass. Final status has zero pins/read
leases and healthy watches/durability. Syslog stream discovery, governed stored
SPARQL, correlation/explanations and full operational acceptance remain unfinished.

## Syslog child task teardown

Signed source `dba5804` fixes the inner syslog join paths. Previously, a failed
listener or TCP child returned immediately from its join loop, dropping the
remaining task set. Listener groups and TCP child groups now use one teardown
owner: publish global stop on failure, join every admitted task and retain all
returned errors and task panics. TCP admission drops its listener before draining
children. TCP/UDP loops and TCP receivers check an already-published stop state
before continuing intake. The daemon's outer teardown and final store fence
therefore follow joined syslog work rather than an aborted child group.

The full release gate and normal signed source hook pass 147 storage scenarios,
58 RPC scenarios, all remaining workspace tests, seeded census self-tests and
229 pinned helper ownership jobs with no distinct exceptions. A controlled
blocking-file publication test proves the join remains pending until the write
finishes, idle children observe stop, and a returned listener error, child panic
and publication-completion error are all retained. A separate real TCP/native
store scenario closes the intake semaphore after durable publication while a
sibling holds a partial frame. The worker reports the admission failure, joins
both connections, and reopens exactly one packet with its original bytes. Its
60-second read timeout exceeds the five-second failure deadline so ordinary
timeout cannot satisfy sibling shutdown. The syslog unit and RPC scenarios also
pass on Btrfs; source identity and six-test log are retained at
`~/.local/state/sparrow/syslog-joins-qualification-v_7ji59n`.

The selected runtime build follows the source hook. Frozen images are
`~/.local/state/sparrow/syslog-joins-images-pgsrhsie`. The closed copied corpus
at `~/.local/state/sparrow/json-pages-qualification-xqp4kox3` preserves all 7,058
record rows, temporal/source/text results, scores, artifact groups, 51 keys and
286 value occurrences against predecessor evidence, with zero leases and a
graceful close.

Public adoption evidence is `/tmp/sparrow-syslog-joins-live._35m1431`, including
the gracefully closed predecessor backup, immutable hashes, capability, unit,
listener and final status. The enabled unit runs PID 167323 on `0.0.0.0:518` and
the existing Unix socket. Schema 45, format 37, store identity and 212 records
are unchanged. Saved finish/handoff retries, TCP/Unix/CLI/MCP grouped and JSON
pagination, native field/time/text witnesses and six idle peers plus CLI status
pass sequentially. Final status has zero pins/read leases and healthy watches
and durability. Public syslog listeners remain unconfigured; private syslog
fixtures provide this change's listener evidence. This closes the child-join
failure gap, not full architecture acceptance: production volume, filesystem
quota, forced physical reclamation, reboot and full governed stored SPARQL
remain unqualified.

## Complete daemon teardown

Signed source `64b69bc` routes owned daemon teardown through one coordinator.
Syslog startup refusal now joins the already-started consume worker, flushes
storage and removes the owned Unix endpoint. Startup socket refusal, terminal
serving/worker failure and graceful shutdown use the same owner. A failed join,
flush or endpoint cleanup does not skip subsequent steps or mask the original
failure. Normal serving teardown retains the RPC LocalSet while intake workers
join. Replacement endpoint names remain preserved.

The full release gate and signed source hook passed release Clippy with warnings
denied, 147 storage scenarios, 57 RPC scenarios, the remaining workspace tests,
seeded census self-tests and 228 pinned helper ledger jobs with no distinct
exceptions. Shared syslog configuration and daemon readiness fixtures have one
implementation. Two new daemon scenarios cover an occupied syslog listener
during startup and a terminal consume-root identity failure together with a
replacement endpoint name. They prove primary and cleanup errors remain visible,
foreign names/listeners survive, corrected configuration reopens the store and
the durable ingest retry retains its receipt. Both scenarios also passed on
Btrfs, with source and log retained at
`~/.local/state/sparrow/shutdown-failure-qualification-ebnxi1sl`.

The selected runtime build followed the source hook; immutable qualified images
are `~/.local/state/sparrow/shutdown-images-zlr54m4s`. The closed copied corpus at
`~/.local/state/sparrow/json-pages-qualification-t8z8vohu` preserves all 7,058
record rows, source/temporal/text results, native scores, artifact groups, 51 keys
and 286 value occurrences exactly against predecessor evidence. It closes
gracefully with zero result leases.

Public adoption evidence is `/tmp/sparrow-shutdown-live.xcj03ce5`. The predecessor
closed gracefully before a closed-store backup and immutable alias replacement.
The enabled unit now runs PID 98545 on `0.0.0.0:518` and the existing Unix socket,
with schema 45, format 37 and the same store identity and 212 records. Saved
handoff/finish intents are retried without fresh imports. TCP/Unix/CLI/MCP
grouped and JSON pagination, temporal/field witnesses, native text scores and six
idle peers plus CLI status pass. Final status has zero pins/read leases and
healthy watches and durability. The first live grouped-driver run overlapped a
JSON driver's lease and failed its global zero-lease assertion; sequential
reruns pass. This qualifies outer daemon coordination, not every possible inner
syslog listener failure or full architecture acceptance. Reboot, production
volume, filesystem quota, forced physical reclamation and full governed stored
SPARQL remain unqualified.

## Durable pressure retirement recovery

Signed source `9103292` fixes two pressure-maintenance invariants. A successful
retirement batch now atomically publishes its linked-file recovery floor and
refused-write estimate alongside the deletions. Reopening restores that wait,
instead of permitting another pass merely because session-local state reset.
Stored-file progress excludes anonymous query allocation/reservations; those
allocations still count toward write admission. If temporary query allocation
alone prevents the requested write, maintenance preserves evidence and waits
for capacity rather than evicting it.

The first durable pressure retirement fences a store at format 38 without a
watch registry, or 39 with one. The shared native publication owner retains that
fence through subsequent watch, consumption and handoff batches and after the
wait clears. Tail ownership checks accept the newer watched format. Older
binaries refuse the fenced formats. Existing stores retain their earlier format
until this durable invariant is first published; wire schema remains 45.
Recovery markers use the existing native catalog decoder, decimal count strings
and a 512-byte bound. Invalid markers or a missing writer fence refuse open.

The full release gate and normal signed source hook passed release Clippy with
warnings denied, 147 storage scenarios, 55 RPC scenarios, all remaining workspace
tests, seeded census self-tests and 225 pinned ledger jobs with no distinct
exceptions. New scenarios cover restart while stored files remain above the
recovery floor, preservation of the next unpinned record, clearing the wait on
capacity recovery, continued format fencing, watch-catalog transitions and
malformed/oversized markers. A real anonymous result lease supplies the separate
query-only-pressure scenario: releasing it restores capacity without retiring
the original evidence. Development corrected a native decimal-string encoding
mismatch and a synchronization return-type mismatch before the final gate.

The selected runtime build followed the source hook. Qualified images are
`~/.local/state/sparrow/pressure-images-afn67zko`. The private Btrfs daemon
qualification is `~/.local/state/sparrow/pressure-wait-qualification-eedm2ij1`.
It ingests 64 distinct log occurrences under a 16 MiB budget with 4 MiB reserve,
then uses owned sparse ballast to trigger admission pressure. One pass retires
32 occurrences and publishes format 39. After increasing that ballast and
closing/reopening, twenty-five observations preserve all 32 survivors with zero
new passes. Every survivor's original bytes are checked; every retired reference
refuses with `EVIDENCE_UNAVAILABLE`. The installed predecessor binary refuses
the closed format-39 store. Removing the fixture ballast permits measured
admission recovery and a durable retry of the previously refused request.
A final reopen retains 33 records, that exact receipt, ready pressure state and
format 39. The public service PID remains unchanged throughout this private run.
Sparse ballast tests accounting/admission and durable wait recovery; it does not
simulate filesystem ENOSPC or certify forced native physical reclamation.

The separate closed copied corpus at
`~/.local/state/sparrow/json-pages-qualification-4mch4gzm` retains all 7,058
ordinary/grouped record rows over 48 pages, source/temporal/text rows and native
scores, 51 keys and 286 value occurrences, exactly matching predecessor evidence.
All result leases are released and the private owner closes gracefully.

Public adoption evidence is `/tmp/sparrow-pressure-live.cmmhwj0j`. The old daemon
stopped gracefully and its closed store was copied before replacement. The
enabled unit started as PID 4018805 with
`~/.local/libexec/sparrow/sparrowd-schema45-9103292`, SHA-256
`d43bc58b99d7639a9a4354c4d6e0bb7794ee093015cc4ec961b8bbd69c7e8412`.
The stable CLI selects `sparrow-schema45-9103292`, SHA-256
`39031f80027a5b8acc7614b060aee7bbf05db62042c41220d97503fd65dfa79f`.
All five installed images match qualification hashes. TCP `0.0.0.0:518`, the
private Unix endpoint, CLI/MCP key/value continuations, grouped record behavior,
saved temporal/field/text witnesses, original finish/handoff intent retries and
six idle MCP peers plus status passed. Final public state retains format 37,
212 records, zero pins/result leases, ready pressure and healthy watches without
pending claims. The public store did not require pressure retirement during this
adoption. `Restart=no`, the sole bind capability and the PurRDF pin are unchanged.

Full governed SPARQL, bounded forced physical reclamation, cancellation latency,
filesystem quota/production-volume acceptance and reboot acceptance remain
unfinished. Historical evidence below remains tied to its original identities.

## Owned local endpoint recovery

Signed source `5ca7eeb` fixes the stale Unix socket that previously prevented
startup after SIGKILL. Startup obtains the native exclusive store lock and
validates the watch catalog before recovering a refused socket. It checks the
socket type without following symlinks, bounds its liveness probe to 250 ms,
and rechecks the original device/inode before removal. Live listeners, timeouts,
other probe errors, non-sockets and replaced names refuse recovery. The private
store directory is the pathname ownership boundary; the check is not a claim
of an atomic compare-and-unlink primitive against an unrelated pathname writer.
Shutdown similarly preserves a replacement name. Socket-bind refusal now joins
the already-started consume worker before returning its error.

The full release gate and normal signed source hook passed all workspace tests,
including 144 storage and 55 RPC scenarios, the new endpoint cleanup unit test,
seeded census self-tests and 225 pinned ledger jobs with zero distinct exceptions.
Focused tests verify durable evidence and stale-cursor refusal after SIGKILL,
restart over TCP and Unix without a test-side unlink, successful graceful socket
cleanup, startup preservation of files/symlinks/foreign live listeners and cleanup
preservation of replacement files and live socket names. Schema remains 45;
this change adds no wire operation or dependency.

The selected runtime build followed the source hook. Qualified frozen images are
`~/.local/state/sparrow/endpoint-images-k169ulmi`. The closed copied corpus at
`~/.local/state/sparrow/json-pages-qualification-axrpjx1w` preserves all 7,058
ordinary/grouped record rows over 48 pages, native source/temporal/text results,
51 keys and 286 value occurrences, exactly matching predecessor receipts and
witnesses. Its `automatic-endpoint-recovery.json` records a held broad capture
with 133,890 resident lease bytes. Admission footprint was 22,660,975 bytes before
capture, 25,171,823 while held and 22,710,101 after SIGKILL/reopen. The restarted
production binary removed its refused stale socket itself; the driver performed
no unlink. All 7,058 records remained, no lease survived and the old cursor failed
with `RESULT_EXPIRED`. The private owner then closed gracefully. Native file/log
changes on reopening do not establish bounded database physical reclamation.

Public adoption evidence is `/tmp/sparrow-endpoint-live.kedvntxw`. The old public
daemon stopped gracefully before its closed store was copied. The enabled unit
started as PID 3820454 with
`~/.local/libexec/sparrow/sparrowd-schema45-5ca7eeb`, SHA-256
`5ff818964e77c1d832001d2a5ee08b4ce073ff9a0c4f1c5be0d8cb7dadf9cd9c`.
The stable CLI selects `sparrow-schema45-5ca7eeb`, SHA-256
`39031f80027a5b8acc7614b060aee7bbf05db62042c41220d97503fd65dfa79f`.
All five installed images match qualified hashes. Public `0.0.0.0:518`, the
daemon's sole `cap_net_bind_service=ep` capability and unchanged `Restart=no`
were recorded. Recovery on a subsequent start does not enable automatic service
restart; the public daemon was stopped gracefully for adoption, not killed.

Live TCP/Unix CLI/MCP key/value continuation, grouped record counts/indices,
release/conflict/empty behavior, saved temporal/field/text witnesses, original
finish/handoff intent retries and six idle MCP peers plus status all passed.
The final public store remains `store:46c88e949ccec658600b66c7becd5bb7`, format 37,
212 records, zero pins/result leases, healthy watches and no pending claims.
Full governed stored SPARQL, bounded physical reclamation, capture concurrency
and production-volume acceptance remain unfinished. Reboot acceptance was not
run. The following sections preserve their historical evidence and limitations.

## Disk-backed JSON query adoption

Signed source `74e0935` routes complete key/value captures through the same
anonymous-file staging owner as record and ranked-text pages. Values stage each
native occurrence directly. Keys finish their native occurrence/presence counts
in a separately bounded 32 MiB working map before staging sorted rows. The host
profile is `sparrow-query-capture/4`; typed receipts advertise maximum 64 MiB
disk capture, subject to store admission, and `keyAggregationBytes` is present
only for key queries. Resident offsets, page limits, deadlines and explicit
release keep their previous bounds. PurRDF projection/matching semantics, the
runtime/census pin and storage format remain unchanged.

The full release gate and normal signed source hook passed release Clippy with
warnings denied, 11 core unit tests, 144 storage scenarios, 53 RPC scenarios,
17 SDK tests, remaining workspace tests, seeded census self-tests and all
224 pinned helper jobs with zero distinct exceptions. One shared dev-only
native-codec fixture supplies both large tests. The core scenario captures
5,002 keys and 5,002 values, each exceeding the earlier 2 MiB result-body limit,
verifies duplicate occurrence versus record-presence counts and original byte
spans, then replays after pin release and evidence retirement. The RPC scenario
traverses 5,001 keys and 5,000 values over TCP and Unix with identical CLI/MCP
continuations and typed admission evidence. Its additional Btrfs run passed at
`~/.local/state/sparrow/json-pages-btrfs.kQY5Pme1`, with a retained qualification
log. Development corrected an unnecessary fixture clone and a source-line-order
test assumption: values retain receipt-time/record-ID, then native occurrence
order. No hook or duplicate detector was bypassed.

The explicit selected runtime build followed the source hook. Frozen images are
`~/.local/state/sparrow/json-pages-images-1cqdfhu3`. Disk qualification is
`~/.local/state/sparrow/json-pages-qualification-3a5isfbq`, cloned from the closed
7,058-record corpus. Complete ordinary/grouped record traversals still return
7,058 rows over 48 pages, exactly matching the original ingestion receipts.
Source, temporal and native ranked text rows/scores/group counts match their
predecessors. The 51 keys and 286 `/component` value occurrences exactly match
the schema-44 baseline at
`~/.local/state/sparrow/json-pages-qualification-if2701y3`, including native
counts and value witnesses. Every lease was released; both private owners closed
gracefully. These checks establish paging correctness on these corpora, not
sustained production-volume or capture-concurrency performance.

Public adoption evidence is `/tmp/sparrow-json-pages-live.5eh9syat`. The old
daemon stopped gracefully and its closed store was copied before alias replacement.
The enabled persistent unit started as PID 3710984 with
`~/.local/libexec/sparrow/sparrowd-schema45-74e0935`, SHA-256
`df0f40cd4f835468111ac5b0b0e2a3a304d76ab7a40c3db4c454f8114b69fe56`.
The stable CLI selects `sparrow-schema45-74e0935`, SHA-256
`39031f80027a5b8acc7614b060aee7bbf05db62042c41220d97503fd65dfa79f`.
All five installed image hashes match frozen qualification. The daemon retains
`cap_net_bind_service=ep`, and `0.0.0.0:518` was verified listening.

TCP/Unix key/value continuations match their CLI/MCP output and schema-44 live
baseline (four keys, twenty value occurrences). Grouped record counts/indices,
release/conflict/empty captures, saved source/time/field witnesses and global text
scores passed. Six idle MCP peers plus CLI status remain usable with four storage
permits. Schema-44 status refuses explicitly during client discovery; schema-42
refusal also remains checked. Saved finish/handoff intent retries preserve the
original receipt, bytes and replacement without importing new evidence. Final
status retains store `store:46c88e949ccec658600b66c7becd5bb7`, 212 live records,
format 37, zero pins/result leases, healthy watches and no pending claims.

Full governed stored SPARQL, bounded physical reclamation, capture concurrency,
production-volume acceptance and automatic stale Unix socket recovery remain
unfinished. Reboot acceptance was not run. Historical evidence below remains
bound to its original source and runtime identities.

## Disk-backed record query adoption

Signed source `6176223` stages immutable chronological, source-time and globally
ranked record summaries one row at a time into an anonymous disk file. Bounded
offsets serve replayable pages; source-time sorting retains only finite sort
keys, and text still uses PurRDF's original complete-corpus scorer. Artifact
counts accumulate over final matching occurrences. No larger resident row vector,
page-local ranker, retained original payload version or implicit pin was added.
The native 10,000-match/scan-work limits remain. JSON discovery retains its
earlier 2 MiB resident capture. See [query streaming](query-streaming.md).

The final normal signed commit hook passed release Clippy with warnings denied,
11 core unit tests, 143 storage scenarios, 52 RPC scenarios, 17 SDK tests, the
remaining workspace tests, census self-tests and the 223-job pinned census gate.
The new large scenario traverses 3,000 field-qualified rows above 2 MiB in each
of chronological, source-time and ranked order; it freezes replay after intake,
checks complete global ranking and source-time order, releases each lease,
refuses disk pressure without a handle, and recovers after that pressure clears.
The anonymous-file unit checks no linked pathname, allocation ownership through
the last Arc, and a typed page failure after an injected truncation. No hook or
detector was bypassed. The final selected runtime build followed the source hook.

Disk qualification is
`~/.local/state/sparrow/disk-records-qualification-_gswvj_y`, cloned from the
previous closed 7,058-record corpus. Both ordinary and grouped broad traversals
return all 7,058 occurrences over 48 pages. Every record ID, parent, source and
receipt-time ordering matches all eight original ingestion receipts. Their
measured complete CLI traversal times were 0.126807 and 0.128614 seconds on this
host; these are one-run observations, not a throughput or production-volume
claim. The 286 source matches, 3,529 temporal matches and 12 text matches retain
exact predecessor rows, scores, witnesses and grouped page counts. Every lease
was explicitly released and the private daemon closed gracefully.

The additional lifecycle check held a complete broad capture with page size one:
resident lease charge was 133,890 bytes, with one anonymous descriptor observed.
The admission footprint went from 22,758,408 to 25,269,256 bytes, including its
conservative anonymous staging charge. One post-capture RSS sample was 93,300 KiB;
this is not a peak or a memory-budget certificate. After SIGKILL and reopening,
the footprint was 22,812,660 bytes, all 7,058 records remained, no result handle
survived, and the old cursor failed with `RESULT_EXPIRED`. Native engine files
and logs can change on reopening; these observations do not certify bounded
physical reclamation of retained database evidence.

SIGKILL leaves the existing Unix socket pathname behind. The first recovery
attempt refused `Address already in use`; the qualified rerun verified its
known-dead private owner and the original socket device/inode before removing
only that fixture socket. Automatic stale-socket recovery remains unimplemented.
The public unit still uses `Restart=no`; graceful stop removes its socket.
Development test failures also exposed an insufficient field-work budget and an
incorrect boolean-as-decoded-string fixture; the final test uses an explicit
100,000-visit budget and native decoded string matching. A misplaced fixture
edit was corrected before the final gate.

Adoption evidence is `/tmp/sparrow-disk-records-live.uu_88yi0`. The old daemon
stopped gracefully before the closed store was copied. The enabled public unit
then started as PID 3335666 with
`~/.local/libexec/sparrow/sparrowd-schema44-6176223`, SHA-256
`43b605eb5cfdc2312e61f9b248bb89ecec0f335b1258813bb0fd76a47998f410`.
The stable CLI selects `sparrow-schema44-6176223`, SHA-256
`e7dd491565c2859e7e1fb43f40c653f060b1fd5a5ac0a1dde90341f359c6934d`.
All five installed images exactly match the qualified frozen images. The daemon
retains only `cap_net_bind_service=ep`; `0.0.0.0:518` was verified listening.
Schema is 44, store format remains 37, and the PurRDF runtime/census pin is unchanged.

The saved public tail-handoff retries preserve the original receipt, exact bytes,
clocks and replacement without another import. Public TCP/Unix grouping and MCP
continuation agree with CLI pages. Temporal/field witnesses and native text scores
match the pre-cutover results exactly. Cursor conflict, release, expiry, empty
groups, strict booleans and schema-42/43 refusal passed. Six idle MCP connections
plus CLI status succeeded under four storage-operation permits. The live store
retains 212 records, no pins, healthy owned watches and zero read handles.

## Shared native read adoption

Signed source `5ae4fd7` routes production chronological selection and field
qualification through the same native backend helpers used by the isolated
snapshot ownership test. It shares matched record summaries between chronological
and ranked queries. This does not yet replace full-row captured result leases;
the bounded streaming integration remains described in
[query streaming](query-streaming.md).

The full `CARGO_BUILD_JOBS=4 make check` and the normal signed commit hook passed:
10 core unit tests, 142 storage scenarios, 52 RPC scenarios, 17 SDK tests,
release Clippy with warnings denied, and the 222-job pinned census gate. The
selected daemon, CLI and producer examples were rebuilt after the hook before
installation. Schema 43, store format 37 and the PurRDF pin are unchanged.

Adoption evidence is `/tmp/sparrow-snapshot-seams-live.xtmqzxot`. The enabled
unit's new PID was 2989337, with immutable daemon image
`~/.local/libexec/sparrow/sparrowd-schema43-5ae4fd7`, SHA-256
`5fed827873ca7f8e90693db88dd2e4cd98e66ba8dd76640426c34d95bfe0ad42`.
The stable daemon and CLI aliases select this source. The daemon retains only
`cap_net_bind_service=ep`; the listener was verified at `0.0.0.0:518`.
The old unit stopped gracefully before its closed store was copied and the
aliases changed. The saved public tail-handoff retry checks passed without
creating another fixture or importing another record. The live store remains
212 records, no pins, healthy owned watches and no read handles after checks.

Pre-cutover results in `/tmp/sparrow-snapshot-fields-before.2olaa9dn` were compared
with the new daemon: all eight source rows, the one received-time/field match,
and the combined field/time/text match retain exact row metadata, occurrence
witnesses, native ranking, score and text spans. Public TCP and Unix grouped
paging preserve counts, indices and ordinary rows; MCP continuation is identical
to CLI continuation. Cursor conflict, explicit release, expired continuation,
empty groups, strict booleans and old-schema refusal also passed. Six idle MCP
connections plus CLI status succeeded with four storage-operation permits.

The independently closed Btrfs corpus clone
`~/.local/state/sparrow/artifact-groups-qualification-yvbywvzx` retained all 7,058
records. Its 286 source matches, 3,529 temporal matches and 12 native text matches
equal the predecessor qualification's rows, text witnesses and artifact groups
across pages. All leases were released and the private daemon closed gracefully.
The broad query still fails explicitly at the 2 MiB capture bound; no limit was
raised. This qualifies the refactor, not completion of broad streaming queries.

Development failures were corrected before the source gate: the new field test's
initial span oracle incorrectly included JSON quotes, and two ledger entrypoint
paths omitted their module. No detector or exception was relaxed. The first
live idle check overlapped the grouping check's temporary leases, so its global
zero-handle assertion failed; the isolated rerun passed after their release.

## Oldest-unpinned logical retirement

`Store::evict_oldest` serializes finite chronological retirement against pin
mutations. It selects records by `(received_at, record_id)` and preserves active
record and whole-artifact pins. Retiring one record makes the unpinned complete
parent unavailable; protected sibling records keep their exact bytes and native
indexes. Shared payload, JSON, RDF and text cleanup uses the existing retirement
owner. Complete source retry receipts retain their original occurrence identity
and do not resurrect evicted bytes.

Each pass limits newly evicted records, time and pin visits, payload ownership
removals, conservative catalog capture and atomic write bytes. Hard capture or batch
refusal commits nothing; a catalog-budget frontier can commit complete retirement
units preceding that boundary. Records retire completely; parent manifest ownership
can be drained over subsequent passes without making a surviving subset
downloadable as a complete file. Reports expose visited/protected counts,
remaining touched-parent manifest references and an explicit traversal frontier.
Logical reference retirement does not certify physical bytes freed or peak RSS.

The daemon now invokes this primitive through the pressure worker below.
Forced native compaction and
sustained physical reclamation qualification remain unfinished.

Temporary-store scenarios cover native RDF/JSON/text and exact-byte survival for
record pins and shared occurrences, stable retries after reopen, whole-artifact
pin protection and expiry, finite manifest cleanup, scan frontiers, atomic
capture/write refusals, and released records whose parent catalog has expired.
The shared corrupted-time-index fixture also checks that a failure after preparing
an earlier retirement commits no successful prefix.

Implementation is in progress in `/home/paudley/Active/sparrow`, on
`implementation/helper-census`. Full architecture acceptance remains unmet.
The current source implements schema 43; the schema sections
below retain their historical qualification evidence.

## Durable file-completion storage foundation

The implementation branch adds a durable producer-finish intent to the existing
file-generation registry. Its immutable final descriptor identity and byte length
bound all later checkpoint publication. Original admission and terminal clocks
survive reopen and exact retries without refreshing existing record deadlines or
minting duplicate occurrences. Terminal publication and its final checkpoint
share the existing record/index WAL batch. Admission alone is not completion.

That foundation used storage formats 29..32. Existing 25..28 registries are validated
and preserved during upgrade; missing checkpoints or unfenced completion
metadata refuse startup. Older executables cannot safely open the new formats.
The schema-38 deployment used format 28 when this foundation shipped. No source
file was finalized or removed by that storage-only change. Schema 39 adds typed
retained completion coordination
as described below. Retained final-file byte coverage and the completed-tail
claim/deletion handoff remain unfinished.

Local `CARGO_BUILD_JOBS=4 make check` passed release Clippy with warnings denied,
135 storage scenarios, 46 RPC scenarios, the remaining workspace tests and the
seeded census suite. The pinned cross-workspace gate passed 193 ownership jobs
with no distinct exceptions. Evidence: `/tmp/sparrow-finish-core-test.log` and
`/tmp/sparrow-finish-gate.log`. These checks qualify the storage foundation,
not a deployed completion service or full architecture acceptance.

## Schema 39 explicit retained completion

Source now exposes `tailFinish` and `tailFinishGet` on the same generated service
over Unix and public TCP. Immutable SDK intents reuse the completed handoff's
strict bounded retry-file container. CLI `file-finish-prepare`, `file-finish`
and `file-finish-status`, plus MCP `file_finish` and `file_finish_get`, use that
one SDK intent and receipt certification path. Local preparation refuses retry
file replacement and destinations inside enabled intake roots through one
shared exclusive, synchronized retry-file writer.

The descriptor-owning worker binds the final file observation under the original
enabled watch revision. Pending completions drain through the existing shared
operation budget and publication owner. Final captures check size and clocks
before and after reads, publish the final unterminated line and terminal checkpoint
atomically, and release the descriptor. An already accepted or empty EOF creates
no duplicate occurrence. Completed retained files are excluded from readmission;
changed terminal files require explicit new producer authority.

Restart resumes pending final observations. Changed and missing originals produce
durable identity/read gaps without resetting checkpoints. A discovered terminal
generation refusal is distinct from deferred scan capacity or lost coverage, so
it cannot prevent another missing generation from receiving its gap. Source
files remain retained; completed-tail claim/deletion and whole-file retained
byte-cover assembly remain unfinished. Full architecture acceptance remains unmet.

The schema-39 service has since been adopted and verified as described below.

The full `CARGO_BUILD_JOBS=4 make check` gate passed release Clippy with warnings
denied, 135 storage scenarios, 48 RPC scenarios, 14 client unit scenarios and
the remaining workspace tests. Seeded census self-tests and the pinned
cross-workspace check passed 199 ownership jobs with zero distinct exceptions.
Evidence: `/tmp/sparrow-finish-service-gate.log`. The real-daemon cases verify
TCP/Unix completion, CLI/MCP receipt agreement, unchanged acceptance clocks,
final unterminated lines, original retry recovery after reopen/source removal,
no completed-file reimport, multi-read pending recovery and independent
changed/missing/empty generation outcomes. Publication uses the existing
operation-budget gate, separately exercised by the tail waiting scenario.

## Schema 39 daemon adoption and retained completion acceptance

The original schema-39 adoption ran `sparrow-development.service` PID **136502**, started at
2026-10-08T10:29:10Z, from the signed functional source **818e020**.
Its immutable executable is
`/home/paudley/.local/libexec/sparrow/sparrowd-schema39-818e020`, SHA-256
`fffaf556ee318d71ae14258f3399a0a96532237a7919deaf2e395d5dcb85464f`.
Its sole file capability is `cap_net_bind_service=ep`. The public Cap'n Proto
listener is **0.0.0.0:518**, with the existing private Unix endpoint at
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`.
The matching installed CLI is `sparrow-schema39-818e020`, SHA-256
`463c3875e51a4bfe4f5559970ae8acbc65efd9aa014729483a359bf77735fbd0`.
The installed SDK events example is `events-schema39-818e020`, SHA-256
`2ed0e6a1c559f995583419b70f237dce9193efc2c0752452cd8444691ace3204`.
All installed hashes were compared with the explicit all-bin/example build after
the signed commit hook completed.

The owned schema-38 process exited successfully, released port 518 and removed
its Unix endpoint before a closed-store snapshot and upgrade. Store
`store:46c88e949ccec658600b66c7becd5bb7`, all 208 previous records, watch revisions
and original generation IDs survived the 28-to-32 migration. The closed snapshot
is `/tmp/sparrow-schema39-live.LdQoqu/store-before-schema39`; restoration was not
exercised, so this is a backup rather than a qualified rollback claim.

The owned watch `development-tail-8a45378`, revision 7, completed its rotated
generation `file-generation:29c37bd9fec7ae98cd412024c061694b` at an already
accepted EOF of 107 bytes/three occurrences. This added no occurrence and did
not change original acceptance clocks. A new owned synthetic JSONL file then
accepted its CRLF-terminated prefix and deferred its final unterminated record
until explicit completion. Generation
`file-generation:e73fa22b7fa8b7ac71bf4012f528e0e2` completed at 52 bytes/two
occurrences. CLI/MCP completion receipts, exact-byte reads, file provenance and
record context agree. Both source files remain intact; no private claim directory
or source removal occurred.

The matching schema-39 SDK retried the original schema-38 public event over
`192.168.2.4:518` twice and then Unix. Every retry returned the original
`artifact:6a81f30ff545ca344c0d579cbffcfdce`, one record/375 bytes. A graceful
schema-39 reopen then recovered both completion receipts with unchanged original
admission/terminal clocks and occurrence IDs. The first read-only verification
started before its Unix socket was ready; after confirming the same new process
active and listening, verification passed without another restart or intake.
Final status remains **210 records**, with one current active held generation,
no tail failure or admission waiting and zero record-read handles.

Live artifacts, saved immutable intents, CLI/MCP inputs and outputs, byte reads,
public retry receipts and service status are in
`/tmp/sparrow-schema39-live.LdQoqu`. `accept.py` creates the owned file and must
not be rerun; `verify.py` only reads and retries the original accepted intents
and event. Evidence does not qualify whole-file retained byte-cover assembly,
completed-tail deletion, forced reclamation, complete SPARQL/correlation/query
contracts, full representative-corpus performance or full architecture acceptance.
The following schema-38 section is historical deployment evidence.

The first evidence commit hook encountered transient native compaction churn in
the existing buffered-byte-limit scenario: admission correctly returned its
documented unstable-footprint `STORAGE_PRESSURE` instead of a partial accounting
sum. That scenario now retries only this exact refusal for at most two seconds,
with the unchanged request and bytes. Capacity and every other failure remain
immediately observable. It additionally asserts one new artifact and one pending
write after synchronization releases capacity. The focused release scenario
passed (`/tmp/sparrow-accounting-test.log`); production accounting and admission
limits were not changed. No hook bypass was used.

## Schema 38 daemon adoption and active file capture

The previous schema-38 service was `sparrow-development.service`, PID **3844123**, started
at 2026-10-08T09:16:37Z. It runs
`/home/paudley/.local/libexec/sparrow/sparrowd-schema38-8a45378`, SHA-256
`12cd4812d65a214e4582997749e71e49907d7280252a4757868fec338616bdeb`, with
the configured public Cap'n Proto binding **0.0.0.0:518** and the existing private
Unix socket. Its sole file capability remains `cap_net_bind_service=ep`.
The matching installed CLI is `sparrow-schema38-8a45378`, SHA-256
`63af6ff85e399dbc550f9dc7220162a25eb6d92079bf141fafedc60f1cd277c8`.
The installed events SDK example is `events-schema38-8a45378`, SHA-256
`85d20ee0cee250d926d6a9a53e22a2c5c3ae61c823f34e7f9169f7cd4ede9758`.

Signed source `8a45378` passed the normal full commit gate. The previous owned
schema-36 daemon exited successfully with MainPID 0, released port 518 and removed
its Unix endpoint before a closed-store snapshot and adoption. Store identity
`store:46c88e949ccec658600b66c7becd5bb7` and all 203 previous records survived
the 24-to-28 store migration. The snapshot and live evidence are in
`/tmp/sparrow-schema38-live.ckt5QQ`.

Typed administration added only the owned synthetic watch
`development-tail-8a45378`, revision 7, rooted at
`/home/paudley/.local/state/sparrow/development-active-8a45378`.
Live capture preserved CRLF, deferred a partial second record, drained an open
rotated descriptor and independently captured its replacement. Four exact-byte
record reads and file spans agree with the original files; CLI/MCP record rows
and context provenance agree. Its two generations remain explicitly incomplete,
covered, descriptor-held and free of observed failures. No claim directory or
source deletion occurred. The initial acceptance driver correctly received
range and concurrent-call refusals; the corrected sequential driver read exact
ranges without replaying intake.

The current SDK example durably ingested over `192.168.2.4:518`; exact TCP and
Unix retries returned artifact `artifact:6a81f30ff545ca344c0d579cbffcfdce`.
Final captured status has **208 records**, zero pins and record-read handles,
no pending writes, and no tail admission waiting. Historical schema sections
below describe earlier qualification rather than the current process.

Recursive tail definitions use explicit `active_files_retain` producer authority,
the existing watch catalog and the shared inotify reconciliation owner. Descriptor
admission, parent/root identities and source observation share the extracted
filesystem helpers with completed consumption. Tail observation creates no
private claim directory. Open current and rotated files remain held within the
finite generation/descriptor quota; round-robin reads feed the original durable
record/checkpoint publication owner. Idle or close does not finalize a partial
line or authorize removal. Observed truncation preserves the terminal old offset
and starts a distinct generation. Missing identities after complete startup
reconciliation produce durable read gaps. Append-only authority does not certify
detection of every source rewrite.

Publication shares the daemon's operation semaphore with RPC and syslog.
Status exposes finite generation IDs, actual capture byte ceiling, held descriptor
count, known-checkpoint record counts and admission waiting. Capture refusal keeps
source bytes and the original checkpoint. Active generations require their
original watch revision and refuse administrative replacement/removal until
resolved. Completed tail handoff is explicitly unfinished.

The full gate passed 133 storage and 46 RPC scenarios, seeded census self-tests
and 192 ownership jobs with zero distinct exceptions. New scenarios cover CRLF,
partial lines, open rotated descriptors, exact record reuse on reopen,
copy-truncation, bounded refusal, symlink exclusion, missing-source recovery gaps
and a fully occupied operation budget. The harness coordinates child creation
with held TCP reservations as well as selecting outside the outbound ephemeral
pool; parallel TCP reopen acceptance passed. Evidence is in
`/tmp/sparrow-active-tail-test.log` and `/tmp/sparrow-tail-engine-gate.log`.

Completed tail handoff, full governed SPARQL and the remaining physical/resource/
recovery qualification remain unfinished. Full architecture acceptance is unmet.

## Schema 37 file-generation checkpoint queries (preceding qualification)

Signed core source `67f168b` publishes complete line prefixes and generation
checkpoints through the existing atomic durable record ingestion path. Partial
lines remain unaccepted until more bytes arrive or completion is explicitly
declared. Retry receipts preserve original occurrences after reopen and after
a declared discontinuity. Original raw paths, watch revisions, namespace/file
identities and half-open byte ranges survive with pinned records. Old writer
families 21..24 migrate to 25..28 with an explicitly seeded checkpoint registry;
missing registry state in a current store is a hard failure.

Schema 37 adds a typed finite generation read and optional record file positions.
CLI `file-generation` and MCP `file_generation_get` share the generated operation
and renderer. Record/context rendering uses the same original declaration codec;
raw relative paths are wire `Data` and agent hex, with no lossy conversion.
Terminal replacement eligibility is separate from retained evidence membership.
Accepted prefixes and unknown continuity do not certify whole-file capture.

The full local gate passed 133 storage and 43 real-daemon RPC scenarios, seeded
census self-tests and 186 ownership jobs with zero distinct exceptions. The new
scenario reads a reopened truncated generation over public TCP, compares CLI/MCP
metadata and context provenance, and refuses foreign stores and stale schemas.
The shared TCP fixture selects outside the native outbound ephemeral pool to
avoid released reservation races with concurrent clients. Evidence is in
`/tmp/sparrow-tail-core-commit.log` and `/tmp/sparrow-tail-wire-gate.log`.

Schema 37 was qualified locally before the schema-38 adoption above. Its typed
queries and immutable record positions are included in that deployed source.
Completed tail handoff and full architecture acceptance remain unfinished.

## Public TCP logging worker

Signed source `2789516` routes the bounded producer worker through the same
explicit `Endpoint::Unix` / `Endpoint::Tcp` selection and discovery owner as
direct clients. `DeliveryConfig::tcp(address, source)` and the tracing example
now support the public listener. Queues, deadlines, immutable pending batches,
store binding and durable flush/shutdown use the existing worker; there is no
parallel logging transport implementation. The normal commit hook passed the
full release gate, including 42 real-daemon RPC scenarios, cancellation of hung
Unix/TCP discovery, 182 ledger ownership jobs and zero distinct exceptions.
The new TCP scenario queues before startup and during an outage, preserves
pending batch/instance/sequence identities across reopen and verifies exact
retry receipts through Unix.

The installed SDK artifacts are
`/home/paudley/.local/libexec/sparrow/sparrow-2789516` (SHA-256
`a23fad45c9e45767ee3c8987ebcb88dc8640299250a4427513a8f695b7fa1fe2`)
and `/home/paudley/.local/libexec/sparrow/tracing-2789516` (SHA-256
`cb3d968b32ee3465c26967f538bb10926ec7eab1aca7e0b963dd353ff9460403`).
Explicitly build both selected bins and examples before capturing artifacts.
The managed build publisher visits the entire leased output directory; an
unselected older CLI was observed in `target/release` after an example-only
build and correctly refused schema 36. Versioned installed acceptance clients
avoid later build publication replacing the executable under test.

Live evidence `/tmp/sparrow-public-tracing-live.x9pdflg9` runs the application-
owned tracing subscriber against `192.168.2.4:518`. Span new/enter, typed log,
exit and close become five durable records with sequences 1..5, zero observed
producer gaps and no drops. Original JSONL preserves boolean `ready`, unsigned
`bytes=64` and unsigned span `attempt=1`; record metadata preserves the producer
positions. CLI/MCP rows agree. Final status has 203 records, zero pins, no handles
and no pending writes. The qualified schema-36 daemon below remains PID 2802541;
this client change requires no daemon lifecycle action. Active-log tailing and
the other full architecture qualification requirements remain unfinished.

## Schema 36 public TCP and syslog (daemon adoption)

Signed implementation `ff6a7a3` adds the public Cap'n Proto listener requested
on 2026-10-08. Configuration binds `0.0.0.0:518`, without authentication.
TCP and Unix use one generated service, connection admission/lease owner,
operation semaphore and storage owner. Rust clients use `Client::connect_tcp`;
the events example accepts `tcp://IP:PORT`. CLI and stdio MCP retain Unix access.

The normal commit hook passed the full release gate: warnings-denied Clippy,
131 storage scenarios, 41 real-daemon RPC scenarios, seeded census self-tests
and the pinned cross gate with 179 ownership jobs and zero distinct exceptions.
Tests cover durable cross-transport retries/reopen, exact syslog packets,
embedded newlines, encoded repeated parameters, malformed interpretation,
framing/connection/deadline/UDP-size refusals and migration preserving producer
windows and watch/handoff fences. Syslog listeners are explicit and disabled in
the development config; their end-to-end acceptance uses isolated real daemons.

After the owned schema-35 PID 2471270 exited successfully under SIGINT,
`sparrow-development.service` adopted schema 36 at 2026-10-08T07:08:37Z,
PID 2802541. The executable is
`/home/paudley/.local/libexec/sparrow/sparrowd-schema36`, SHA-256
`fcfb0d5fe1dc063889fd8d1a7ab2902236696c5eefd6c9f8bfe57680a9ee6586`.
Its sole file capability is `cap_net_bind_service=ep` for port 518. The installed
and running binary hashes agree. Config remains `config/development.toml`;
Unix remains `/home/paudley/.local/state/sparrow/development-v5/rpc.sock`.
The existing store `store:46c88e949ccec658600b66c7becd5bb7` reopened under
`sparrow.store/24` with all 197 pre-existing records preserved.

Live evidence in `/tmp/sparrow-public-tcp-live.uopb00va` publishes one durable
typed event through `192.168.2.4:518`. Exact TCP and Unix retries return the same
artifact `artifact:19b3f05f9dfa8803d3f24806d164a735` and record
`record:49ee5bde6185bf1e5bc14d5f07ce3395`; exact-byte inspection preserves sequence
1. Final status has 198 records, no active pins, no handles in any registry and
no pending writes. This verifies the host's LAN interface, not external routing.

Regression evidence in `/tmp/sparrow-context-live.cc6za0su`,
`/tmp/sparrow-key-path-live.sx7o818w`, `/tmp/sparrow-result-next-live.dvge0vju`
and `/tmp/sparrow-artifact-filters-raw-regression.tiry_jen` preserves CLI/MCP
equality, partial/unknown scope evidence, all eight frozen continuation shapes,
82-byte descriptor and 87-byte CRLF handoff bytes and the producer replacement.
`/tmp/sparrow-artifact-scope-schema35-compat.q1n_yets` verifies ten current client
operations refuse the archived schema-35 daemon with UNSUPPORTED and no stdout;
the isolated child exits cleanly and removes its socket.

Live tailing, full governed SPARQL and full physical/resource/recovery
qualification remain unfinished. Full architecture acceptance remains unmet.

## Schema 35 structured producer windows (preceding adoption)

Typed event ingestion persists finite producer windows and immutable record
positions through the existing shared preparation pipeline. The registry,
occurrences, native RDF metadata and retry declaration commit in the same WAL
batch. Exact retries preserve occurrence identity after newer batches and
reopen; overlapping new sequences refuse without advancing the checkpoint.
Observed gaps count absent intermediate positions. Prefix/trailing continuity
and the cause of absence remain unknown. Recent intervals have an explicit
completeness flag; aggregate counts stay exact when older intervals leave the
bounded suffix.

Source discovery exposes retained window IDs. The generated producerStreamGet
operation, CLI producer-stream and read-only MCP producer_stream_get share one
typed client/rendering path. Record metadata includes an optional position;
ordinary and historical unpositioned uploads are not reinterpreted. Finite
stream summaries expire independently of record pins; a later observation uses
a new opaque identity. Native RDF metadata uses PurRDF's dataset builder and is
verified by a governed native SELECT over the stored view.

The release gate passes 128 storage scenarios and 39 real-daemon RPC scenarios,
seeded census self-tests and the pinned cross-workspace gate with 173 ownership
jobs and zero distinct exceptions. Migration scenarios preserve exact bytes,
diagnostic mappings, store identity and watch/handoff authority through families
13..16 into 17..20. Missing or malformed producer registries refuse startup.
The existing events example can send an explicit sequence for a declared
instance; its exact retries retain all arguments.

Isolated compatibility evidence in
/tmp/sparrow-artifact-scope-schema34-compat.fcn3p10k verifies ten current-client
operations refuse the archived schema-34 daemon with UNSUPPORTED and no stdout.
That archived executable is SHA-256
b60962b4d917abee4e9cdc03aa083b4de76c01ef57c960529399cb4641330e14;
the owned child exits cleanly and removes its socket.

Qualified functional source is signed commit 734ca96. The owned development
service adopted schema 35 at 2026-10-08T06:16:25Z (PID 2471270), retaining
store:46c88e949ccec658600b66c7becd5bb7 and its watch/handoff authority in
sparrow.store/20. Its private socket remains
/home/paudley/.local/state/sparrow/development-v5/rpc.sock with directory/socket
modes 0700/0600. The old PID exited cleanly before this adoption.

Running acceptance in /tmp/sparrow-producer-window-live.wro4yyp_ uses the built
Rust event client example to publish sequences 1 and 4 under one instance.
One durable batch and one buffered batch followed by typed flush-through produce
two independent records and one window, producer-stream:62bad75bb26f0a96dc72f5142198569a.
The summary reports exactly two unobserved positions, the inclusive gap 2..3,
and unknown continuity. CLI and MCP summaries agree. Exact retries of both
batches reuse their artifact and record IDs without advancing counters; a new
overlapping sequence fails CONFLICT and leaves the checkpoint unchanged.
Record byte inspection agrees with each normalized producer sequence; historical
records remain explicitly unpositioned. Foreign-store, absent-window and unknown
MCP-field refusals are also verified. Final status has 197 retained records,
zero active pins, zero handles across all four registries and zero pending writes.

Context regression evidence in /tmp/sparrow-context-live.w0z7bd08 preserves
CLI/MCP row equality, partial scan evidence, unknown diagnostic membership and
strict refusal. Exact-byte evidence in
/tmp/sparrow-artifact-filters-raw-regression.phxflk78 preserves the 82-byte
descriptor, 87-byte CRLF handoff, original IDs, removed handoff phase and
replacement producer file. Key-path evidence in
/tmp/sparrow-key-path-live.t6def1kd preserves root/path counts, invalid-pointer
refusal and unknown-scope coverage. All eight query/result shapes in
/tmp/sparrow-result-next-live.rljg0in6 preserve frozen original/generic CLI/MCP
continuations after pin mutation and release. The key cursor deadline
2026-10-08T06:17:31.957Z and released-pin cursor deadline
2026-10-08T06:17:32.103Z expire with RESULT_EXPIRED and no stdout.
Live tailing, syslog, full governed SPARQL and full architecture acceptance
remain unmet.

## Schema 34 exact key path discovery

Key discovery accepts an optional exact native RFC 6901 path in the core,
typed RPC, CLI and MCP. Root and absent paths remain distinct. Escaped member
names, duplicate occurrences, kinds and record-presence counts retain native
semantics. The existing occurrence predicate now serves both key and value
selection. Native pointer syntax validation is adopted from PurRDF. Typed JSON
paths and query cursors share one bounded decoder before allocation; the core
also enforces the JSON path byte ceiling. Captured path selectors and counts
remain frozen across original and cursor-only continuation.

The host query admission profile advances to sparrow-query-capture/2 and reports
query_path_bytes for JSON captures. Other phases report explicit absence.
The full source gate passes release Clippy with warnings denied, 125 storage
scenarios, 39 RPC/daemon scenarios, seeded census self-tests and the pinned
cross-workspace gate (171 ledger jobs, zero distinct exceptions). New core
scenarios compare exact paths, duplicate/kind presence counts, root/empty-member
spelling, native escape syntax, byte admission and frozen counts after ingestion.
The new RPC scenario verifies generated capture limits, CLI/MCP agreement,
path-bound continuations, cursor-only replay and refusals without rows.

The signed feature commit `8e8399f` passed the complete verification hook. The
owned development unit reopened this build at `2026-10-08T05:34:33Z`, PID
2190724, after graceful exit, successful inactive state and socket removal of
owned PID 2028003. The same private Unix socket, config and persistent store
remain in use: `store:46c88e949ccec658600b66c7becd5bb7`, format
`sparrow.store/16`, with 195 live records.

Live evidence in `/tmp/sparrow-key-path-live.ilsf1h4r` compares CLI/MCP message,
root and level counts, exact absent-path results with scope coverage, frozen
unrestricted continuation and refusal when its path changes. Invalid syntax and
oversized paths refuse without rows. Unknown diagnostic membership remains
incomplete; strict mode refuses INDEX_NOT_READY without stdout. This sample has
one kind per tested exact path; the release RPC scenario separately proves
multi-kind exact-path pagination and frozen duplicate counts after later ingestion.

Compatibility evidence in `/tmp/sparrow-artifact-scope-schema33-compat.jnmtd9v7`
checks nine operations, including path-restricted keys, against the archived
schema-33 daemon, SHA-256
`17343965f3853f963dfdcfa32e124794d3158daa4fea0d57f83f8dbb73e7bb75`.
All refuse UNSUPPORTED with empty stdout; the isolated daemon exits cleanly and
removes its socket. Context regression evidence in
`/tmp/sparrow-context-live.0o7hn3j8` checks CLI/MCP agreement, capture limits,
partial frontiers and strict unknown refusal without creating handles or pins.
Raw regression evidence in `/tmp/sparrow-artifact-filters-raw-regression.st62umtm`
compares exact 82-byte descriptor and 87-byte CRLF handoff downloads, stable
artifact identity, removed handoff phase and preserved producer replacement.
Query regression evidence in `/tmp/sparrow-result-next-live.nvmdfynb` compares
all eight leased page shapes through original and CLI/MCP cursor-only
continuation, including frozen receipts after pin expansion and release.
After their fixed absolute deadlines, both the key discovery cursor and the
released-pin query cursor refuse RESULT_EXPIRED without stdout. Final read
handles across all four registries, active pins and pending writes are zero;
all 195 records remain. Directory/socket modes are 0700/0600.
Full architecture acceptance remains unmet.

## Schema 33 bounded single context capture

Context uses one conservative catalog allowance for the anchor and both
chronological directions, and one reply allowance for all output rows. Every
visited catalog entry is charged before decoding, including skipped sources.
The response row charge is shared with immutable query pages. Byte exhaustion
refuses the operation without a successful prefix, evidence mutation or handle.
Directional visit frontiers retain their existing visible partial semantics.

The generated ContextCapture carries a fresh single-reply identity, eligibility
instant, host admission profile, explicit native governor absence and actual
limits. CLI and MCP use one adapter for the capture receipt; original certainty,
diagnostic coverage, record mapping profiles and continuity evidence remain.
The full source gate passes release Clippy with warnings denied, 123 storage
scenarios, 38 RPC/daemon scenarios, seeded census self-tests and the pinned
cross-workspace gate (170 ledger jobs, zero distinct exceptions). New core
scenarios exercise a combined reply refusal across both directions and aggregate
catalog admission over skipped sources. The new RPC scenario checks generated
capture fields, a partial scan frontier, CLI/MCP byte refusals without rows and
an accepted reply within the shared output ceiling.

The signed feature commit `d238b43` passed the complete verification hook.
`f24d3ed` makes the frontier acceptance scenario independent of same-millisecond
clock ties and also passes the full hook; production sources are unchanged. The
owned development unit reopened the feature build at `2026-10-08T05:09:24Z`, PID
2028003, after graceful exit, successful inactive state and socket removal of
owned PID 1973781. The same private Unix socket, config and persistent store
remain in use: `store:46c88e949ccec658600b66c7becd5bb7`, format
`sparrow.store/16`, with 195 live records.

Live context evidence in `/tmp/sparrow-context-live.ckls7731` compares CLI/MCP
rows, ordering and continuity. Both report distinct single-capture identities,
matching eligibility times, typed host limits and explicit governor absence.
A one-visit directional frontier reports partial execution and its stop reason.
Unknown diagnostic membership retains its original incomplete certainty;
strict mode refuses INDEX_NOT_READY with no stdout. Context creates no handles
or pins and leaves all 195 records intact.

Compatibility evidence in `/tmp/sparrow-artifact-scope-schema32-compat.gyid0l7n`
checks nine operations against the archived schema-32 daemon, SHA-256
`d83a2728e22c7cab0cd2cc69ec853714ec4ceed4a95ddfb907795ad8ab60ce68`.
Every current-client operation refuses UNSUPPORTED with empty stdout; the
isolated daemon exits cleanly and removes its socket. Raw regression evidence in
`/tmp/sparrow-artifact-filters-raw-regression.ory4yles` compares exact 82-byte
descriptor and 87-byte CRLF handoff downloads, stable artifact identity, removed
handoff phase and preserved producer replacement. Query regression evidence in
`/tmp/sparrow-result-next-live.4lg3d1az` compares all eight existing leased page
shapes through original continuation and CLI/MCP cursor-only replay, including
frozen receipts after pin expansion and release. After the absolute deadline
`2026-10-08T05:10:39.037Z`, the released-pin cursor refuses RESULT_EXPIRED
without stdout. All four read registries report zero handles; pins and pending
writes are zero and all 195 records remain. Directory/socket modes are 0700/0600.
Full architecture acceptance remains unmet.

## Schema 32 frozen query capture provenance

Record, standalone text, key and value receipts now carry typed capture
identity, native role-tagged profiles and effective host phase ceilings.
The core freezes evidence alongside native result metadata and the shared wire
receipt owner encodes it once for direct, nested text and cursor-only replies.
One agent adapter augments receipts while preserving their original certainty
and coverage; pagination remains separate. Unused phases and native governor
evidence are explicitly absent. Host admission is identified independently of
native semantic profiles. Shared limit constants also enforce the reported caps.

The full source gate passes release Clippy with warnings denied, 121 storage
scenarios, 37 RPC/daemon scenarios, seeded census self-tests and the pinned
cross-workspace gate (168 ledger jobs, zero distinct exceptions). New core
evidence checks native profile identity, applicable limits and frozen evidence
after ingestion; the RPC scenario checks all four paged query receipts through
CLI, MCP and cursor-only continuation.

The signed source commit `86fee32` passed the complete verification hook. The
owned development unit reopened this build at `2026-10-08T04:58:11Z`, PID
1973781, after graceful exit, successful inactive state and socket removal of
owned PID 1818673. The persistent store remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and directory/socket modes 0700/0600.

Live evidence in `/tmp/sparrow-result-next-live.ajcsexav` checks all eight
existing page shapes against original continuation and CLI/MCP cursor-only
replay. The four query surfaces report frozen native profile roles, capture
identity, applicable limits, explicit governor absence and unchanged semantic
receipts. Query replay remains identical after pin expansion and release.
Foreign, malformed, zero-boundary and unknown cursors refuse without rows.
After the absolute deadline `2026-10-08T04:59:15.708Z`, the released-pin cursor
refuses `RESULT_EXPIRED` without stdout. Final query handles, active pins and
pending writes are zero; all 195 live records remain.

The archived schema-31 daemon is SHA-256
`62ae76a5c928d90abae96e57769f35661e6513e1e467d5acbebe9db02e441278`.
Compatibility evidence in `/tmp/sparrow-artifact-scope-schema31-compat.f1fm4tu5`
checks eight current-client operations: each refuses `UNSUPPORTED` with empty
stdout; the isolated old daemon exits cleanly and removes its socket. Raw
regression evidence in `/tmp/sparrow-artifact-filters-raw-regression.aw_5o2wo`
compares exact 82-byte descriptor and 87-byte CRLF handoff downloads, stable
artifact identity, removed handoff phase and preserved producer replacement.

Incomplete diagnostic scope retains one unknown member, null total eligible
count and original incomplete certainty with no native governor claim. Full
architecture acceptance, certified native SPARQL receipts and context capture
provenance remain unfinished. Historical qualification follows.

## Schema 31 cursor-only result continuation

The core, typed daemon, CLI and MCP now resume captured results without repeating
selectors. ResultPage preserves each existing operation through a typed union;
pin collections/members retain their nested union. All original operations and
result_next use the same wire encoders and canonical agent renderers. Query
replies include their operation identity. Continuation does not re-evaluate
membership, pin names, watch configuration, native predicates or ranking.

Cursor parsing and page-boundary/expiry staging are owned by the existing
catalog lease module. Captured-selector inspection borrows the bounded stored
selector rather than allocating another copy. Store mismatch, invalid positions
and expired/unknown/previous-owner cursors refuse without successful rows. No new
capture or retention protection is created and no absolute lease is renewed.

The full source gate passes release Clippy with warnings denied, 120 storage
scenarios, 37 RPC/daemon scenarios, seeded census self-tests and the pinned
cross-workspace gate (167 ledger jobs, zero distinct exceptions). New scenarios
compare original continuations with cursor-only CLI/MCP replies and typed wire
union identity for all eight existing page shapes, including both pin variants.
Core evidence also covers released-pin replay, foreign/unknown/previous-owner
handles, malformed and variable byte-page positions, and fixed absolute expiry.
The MCP catalog exposes 24 tools; result_next has a closed read-only input.

The feature source commit is `852ccff`; `2db5740` corrects record reply operation
placement and strengthens the canonical renderer scenario to check root labels
and field witnesses. Both signed commits passed the full verification hook.
The owned development unit reopened the corrected build at
`2026-10-08T04:35:29Z`, PID 1818673, after graceful exit, successful inactive
state and socket removal of the preceding owned PID 1759754. The same config,
socket and store identity remain in use: `store:46c88e949ccec658600b66c7becd5bb7`,
format `sparrow.store/16`, 195 live records; directory/socket modes are 0700/0600.

`/tmp/sparrow-result-next-live.eg1zyfj9` records cursor-only CLI/MCP equality with
original continuations for record, text, key, value, artifact, source, collection
and direct-member pages. Captured totals are respectively 3, 3, 7, 3, 2, 16, 2
and 2. Operation identities, snapshot/as-of, limits, coverage and semantic
receipts are preserved. A revision-guarded pin captures two message values;
subsequent expansion and release leave its cursor-only result unchanged.
Foreign store, malformed cursor, zero position and unknown handle refuse with
typed errors and empty stdout. All temporary pins are released; the live
record population is unchanged and pending writes are zero.
The released-pin result cursor refuses `RESULT_EXPIRED` after its original
absolute deadline `2026-10-08T04:36:29.837Z`; intervening original and cursor-only
replays did not renew it. Final status has zero query/artifact/source/pin result
handles, active pins and pending writes.

`/tmp/sparrow-artifact-scope-schema30-compat.4a9jhcem` verifies current
result-next/status/key/value/record/text/source/artifact refusal against the
archived schema-30 daemon: exit 1, `UNSUPPORTED`, empty stdout. Archive SHA-256:
`2027b82a82b924befb9712f47a4cc9bf43b79f4de520121eb2209f35dcd48af4`.
The private compatibility child exits cleanly and removes its socket.
`/tmp/sparrow-artifact-filters-raw-regression.33h_qe_a` verifies exact 82-byte
descriptor and 87-byte CRLF handoff reconstruction, stable identities, removed
handoff phase and preserved producer replacement.

Full architecture acceptance remains unmet. Complete scope receipts, indexed
corpus-scale selection, the recorded upstream governed SPARQL seam, stream/tail/
syslog generation and gap contracts, and full physical/resource/recovery
qualification remain required. The schema 30 section below preserves preceding
runtime evidence.

## Schema 30 JSON discovery result pages

Keys and values now capture complete admitted native JSON discovery through the
same immutable query lease/body owner as records and text. Schema 30 carries
typed cursors and page receipts; CLI and MCP expose cursor/page-size and refuse
both limit aliases together. Counts, occurrence order and coverage remain frozen
after ingestion, membership changes or retention; cursors bind the operation
and all selectors. Scan, row, capture and aggregate handle exhaustion refuse
before publication. Native PurRDF JSON projection, occurrence predicates and
codecs remain the semantic owners. The storage format remains unchanged.

`CARGO_BUILD_JOBS=4 make check` passes release Clippy with warnings denied,
119 core storage scenarios, 36 RPC/daemon scenarios, seeded census self-tests
and the pinned cross-workspace gate (164 ledger jobs, zero distinct exceptions).
The new scenarios cover more than 1,000 keys/values, complete key counts,
duplicate members, frozen CLI/MCP replay, cross-operation/predicate refusal,
shared handle ceilings and fixed expiry, and replay after pin release and native
projection retirement. Pagination changes the old limit-truncation scenario to
require complete execution plus a continuation. Equal receipt timestamps are
ordered by record ID; the test verifies occurrence identity rather than assuming
line order across records.

The signed source commit is `2a7b7d6`. The owned development unit was adopted
after graceful exit of PID 1445461, successful inactive state and socket removal.
The same unit/config/store reopened under schema 30 at
`2026-10-08T04:12:39Z`, PID 1619807. Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records. The directory/socket retain modes 0700/0600.

Live evidence in `/tmp/sparrow-json-pages-live.hoid5ql6` captures all seven
observed key/kind rows and three message values from the owned diagnostic source
through one-item pages. Native counts and occurrence rows match the unpaged
baseline; continuation CLI/MCP replies are identical, with frozen receipts.
Key/value cursor crossover, changed value path, scan exhaustion and conflicting
limit aliases refuse without successful stdout. Both JSON surfaces preserve
one unknown diagnostic membership and null eligible counts; strict reads refuse
`INDEX_NOT_READY`. A revision-guarded pin captures two message occurrences;
membership expansion then release does not alter its captured continuation.
Fresh stale/released scopes refuse. Record/text native rows and ranking agree.
The captured pin cursor refuses `RESULT_EXPIRED` after its absolute deadline
`2026-10-08T04:13:41.008Z`, without renewal. Final status has zero pins, pending
writes and query/artifact/source/pin read handles.

`/tmp/sparrow-artifact-filters-raw-regression.2smyxdxj` verifies the retained
82-byte descriptor and 87-byte CRLF handoff artifact exactly, their stable retry
identities, removed handoff phase and preserved producer replacement.
`/tmp/sparrow-artifact-scope-schema29-compat.5e9izmxl` records current client
status/key/value/record/text/source/artifact refusal against the archived prior
daemon: each exits 1 with `UNSUPPORTED` and empty stdout. Archive SHA-256 is
`694321bbad921c52fc30ac2c5c9bf5cd78087561b99bc821335da3ea498319d0`;
the private compatibility child exits cleanly and removes its socket.

Full architecture acceptance remains unmet. Complete scope receipts, indexed
corpus-scale selection, the recorded upstream governed SPARQL seam, stream/tail/
syslog generation and gap contracts, and full physical/resource/recovery
qualification remain required. The schema 29 section below retains preceding
runtime evidence.

## Schema 29 standalone text result pages

The production `textSearch` path uses the record result capture and native ranker,
with typed `TextSearchPage` metadata and original typed text results. CLI and MCP
accept cursor/page-size options and share their renderer and pagination projection
with record queries. Page size no longer truncates text execution.

The capture selector binds its operation, so record and standalone text cursors
cannot cross surfaces even for equivalent predicates. Both use the same eight
handles, 8 MiB retained ceiling, 2 MiB captures, 256 KiB pages and absolute
60-second leases. Scope, ranking corpus, native scores and coverage remain frozen.
Scan/item/byte exhaustion refuses without publishing a prefix. Standalone text
admits its declared text/scope surface; source-time and same-record field predicates
remain on record search. Storage formats remain unchanged.

Local qualification passed formatting, release Clippy with warnings denied,
workspace release tests (116 storage and 35 RPC/daemon/MCP scenarios), seeded
census self-tests and the pinned cross-workspace gate. All 161 ownership jobs
hold with zero distinct exceptions. Evidence is
`/tmp/sparrow-text-pages-gate-20261007.log`. Tests cover complete capture beyond
1,000 ranked rows, frozen scores/corpus after new ingestion, CLI/MCP replay,
cross-operation refusal, shared record/text handle admission and absolute expiry.
Signed implementation `2549dde` passed the normal full commit hook; evidence is
`/tmp/sparrow-text-pages-commit-20261007.log`.

The owned schema-28 unit exited successfully under SIGINT, with PID zero and
socket removal, before the same unit/config/store reopened under schema 29 at
2026-10-08 03:49:15 UTC (observed PID 1445461). Executable, config and private
socket remain `target/release/sparrowd`, `config/development.toml` and
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`. Store identity is
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`; all 195 live
records remain, with zero active pins and no pending writes. Directory/socket
modes remain 0700/0600.

`/tmp/sparrow-text-pages-live.tn_4xjte` verifies all three ranked source records
through one-item text pages, identical CLI/MCP continuation, native score/corpus
equality with record search, and cross-operation refusal in both directions.
Unknown diagnostic membership remains visible with null semantic counts; strict
and scan refusal produce no stdout. Text capture of a revision-1 two-record pin
keeps its original receipt and ranking after revision-2 expansion and release.
The text cursor subsequently expires with `RESULT_EXPIRED`, exit 1 and no stdout;
reading pages does not renew the lease.

`/tmp/sparrow-record-pages-live.9ercgw2x` requalifies record pagination, pin
mutation/release, diagnostic coverage and CLI/MCP replay. Source inventory in
`/tmp/sparrow-source-wire-live.9f47wila` preserves all 16 sources and mapping/watch
counts. Artifact filters and frozen replay pass in
`/tmp/sparrow-artifact-filters-live.nilkho7o`. Raw regressions in
`/tmp/sparrow-artifact-filters-raw-regression.831w637k` preserve stable descriptor
and handoff identities, exact 82/87-byte exports, removed phase and producer
replacement. `/tmp/sparrow-text-pages-regression.tc2qluel` preserves native text,
record and context diagnostic coverage, strict refusal, CLI/MCP agreement and
exact 104-byte stable retry/export.

The current client refuses status, keys, records, text, sources and artifacts
against the private archived schema-28 daemon with `UNSUPPORTED`, exit 1 and zero
stdout. That owned child exits cleanly and removes its socket. Evidence is
`/tmp/sparrow-artifact-scope-schema28-compat.9cqbf0lb`; archive SHA-256 is
`8adb2cc0dc6b509f3e4e41e9e955a52844f335cb6c52b13f2a6f4dbfb095901d`.
Standalone key/value pagination and the remaining architecture acceptance work
remain unfinished.

## Schema 28 record result pages

The production `recordsSearch` path captures the complete admitted result through
the existing same-record matcher and native text ranker. Page item limits no longer
truncate execution. Candidate scan, item or byte exhaustion refuses the operation
without a prefix. Native query coverage and diagnostic uncertainty are preserved.

Cap'n Proto, CLI and MCP carry the same frozen capture, including ranking corpus,
scores, source-time basis and field witnesses. Captured encoded metadata/rows use
PurRDF's native JSON codec and share the finite catalog lease/cursor owner. Byte
admission chooses variable page boundaries and accounts duplicated record/text
rendering. Store clones share eight handles and an 8 MiB retained ceiling, with
fixed 60-second expiry; the capture ceiling is 2 MiB. Reading pages does not pin
records or keep storage versions alive. Source/artifact/pin pages retain their
existing fixed-boundary contracts.

Local qualification passed formatting, release Clippy with warnings denied,
workspace release tests (114 storage and 34 RPC/daemon/MCP scenarios), seeded
census self-tests and the pinned cross-workspace gate. All 160 ownership jobs
hold with zero distinct exceptions. Evidence is
`/tmp/sparrow-record-pages-final-gate-20261007.log`. Tests cover capture beyond
1,000 rows, frozen native ranking and uncertainty, pin release/retirement,
absolute expiry/reopen, variable byte boundaries, scan/row refusal without a
prefix, and shared handle ceilings. Signed implementation `f4ec8d0` passed the
normal full commit hook; its evidence is
`/tmp/sparrow-record-pages-commit-20261007.log`.

The owned schema-27 unit exited successfully under SIGINT, with PID zero and
socket removal, before the same unit/config/store reopened under schema 28 at
2026-10-08 03:31:45 UTC (observed PID 1354944). The executable remains
`target/release/sparrowd`, config `config/development.toml`, and private socket
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`. Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with all 195
live records, zero active pins and no pending writes. Directory/socket modes
remain 0700/0600.

`/tmp/sparrow-record-pages-live.zvmy4zai` verifies all three ranked source records
through one-item pages, exact equality with the native standalone text query's
scores and corpus, and identical CLI/MCP replay. A revision-1 pin capture retains
its two-record membership after revision-2 expansion and release. Fresh stale or
released pin scopes refuse; continuation keeps the original semantic receipt.
Unknown diagnostic counts remain null with a one-record lower bound and visible
unknown coverage. Strict and scan refusal return exit 1 without stdout.
The original pin cursor subsequently refuses with `RESULT_EXPIRED` and no stdout,
and status reports zero record result handles. Reading pages did not renew it.

`/tmp/sparrow-source-wire-live.m_bwuyev` verifies all 16 source pages, mapping and
watch declarations, and unchanged occurrence counts. Artifact default/strict
coverage, selected records and frozen page replay pass in
`/tmp/sparrow-artifact-filters-live.l1g4b8t8`. Raw regressions in
`/tmp/sparrow-artifact-filters-raw-regression.sld87sje` preserve descriptor/handoff
identities, exact 82/87-byte exports, removed phase and producer replacement.

`/tmp/sparrow-record-pages-regression.gft34z1u` preserves diagnostic record,
standalone native text and context coverage, strict refusal, CLI/MCP agreement
and exact 104-byte stable retry/export. Final record-handle evidence is
`/tmp/sparrow-record-pages-status-final-20261007.json`.

The current client refuses status, keys, records, sources and artifacts against
the private archived schema-27 daemon with `UNSUPPORTED`, exit 1 and zero stdout.
That owned child exits cleanly and removes its socket. Evidence is
`/tmp/sparrow-artifact-scope-schema27-compat.u7_tf3de`; archive SHA-256 is
`d172cfb60b7e1333c3ec1e08b84bd34766f00434418b30230159c36223d79f14`.
Standalone key/value/text pagination, complete scope receipts, streams and the
other architecture acceptance work remain unfinished.

## Schema 27 artifact scope filters

Artifact discovery uses the shared typed scope and agent scope codec for source
alternatives and diagnostic dimensions. Original artifact receipt filtering runs
before the existing record diagnostic matcher. Rows require proven matches when
diagnostic filters are present; retained membership counts remain exact.
Shared typed coverage carries matching-record lower bounds and unknown mappings.
Unknown-only parents contribute capture coverage even without a proven row.
Matching artifact totals remain unknown when coverage is incomplete, and strict
CLI/MCP admission refuses rather than publishing a prefix.

Continuation freezes aggregate and per-parent coverage and binds all filters,
strictness and membership. Source discovery still uses an unfiltered artifact
capture and keeps exact retained occurrence counts. Storage formats remain
unchanged. Local qualification passed formatting, release Clippy with warnings
denied, workspace release tests (110 storage and 32 RPC/daemon/MCP scenarios),
seeded census self-tests and the pinned cross-workspace gate. All 157 ownership
jobs hold with zero distinct exceptions. Evidence is
`/tmp/sparrow-artifact-filters-gate-20261007.log`. The normal commit hook verifies
the final source, including CLI help and removal of the obsolete restricted scope
schema mode. Signed implementation `a97b695` passed that normal full gate.

The owned schema-26 unit exited successfully under SIGINT, with PID zero and
socket removal, before the same unit/config/store reopened under schema 27 at
2026-10-08 03:10:27 UTC (observed PID 1285304). The executable remains
`target/release/sparrowd`, config `config/development.toml`, and private socket
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`. Store identity is
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`; all 195 live
records remain and directory/socket modes are 0700/0600.

Live evidence in `/tmp/sparrow-artifact-filters-live.38pormiv` proves one known
matching artifact alongside one unknown record, incomplete total/coverage,
strict refusal with no stdout, exact selected-record strict filtering, source
intersection before strict evaluation, and equal frozen CLI/MCP page replay.
`/tmp/sparrow-artifact-scope-live.jpgx7ef0` requalifies pin revision changes,
release and immutable continuation, overlap deduplication, scoped record counts
and zero active pins. The shared handle assertion checks its 1..8 ceiling rather
than assuming no other admitted captures exist.

`/tmp/sparrow-source-wire-live.cnu4qrj0` verifies all 16 source pages and unchanged
mapping/watch/occurrence counts. `/tmp/sparrow-artifact-filters-regression.x9j1hxb_`
preserves record, native text and context default/strict coverage, CLI/MCP
equivalence and exact 104-byte retry/export. Raw regressions in
`/tmp/sparrow-artifact-filters-raw-regression.yi1admhf` preserve descriptor/handoff
identities, exact 82/87-byte exports, removed handoff phase and producer replacement.

The current client refuses status, keys, sources and artifacts against the
private archived schema-26 daemon with `UNSUPPORTED`, exit 1 and zero stdout.
That owned child exits cleanly and removes its socket. Evidence is
`/tmp/sparrow-artifact-scope-schema26-compat.4sboem7d`; archive SHA-256 is
`2aeef354b1a7f5d339e07dac3b59c865722f8937f1eec667b35fa449494e6a86`.
Commit-hook evidence is `/tmp/sparrow-artifact-filters-commit-20261007.log`.

Broader query pagination, indexed corpus-scale selection, producer stream
generation/gap operations, full governed SPARQL integration, and physical
resource/recovery qualification remain required. This is development acceptance
for artifact scope filtering, not completion of the server architecture.

## Schema 26 artifact membership

Artifact discovery admits the existing store-scoped live/evidence/pin vocabulary
through typed `ArtifactListQuery.selection`, CLI `artifacts --scope-file FILE`
and MCP `artifacts_list.scope`. One reference owner serves record queries and
artifact discovery, preserving whole-artifact and recordless identities alongside
deduplicated records and parents. Source/receipt filters run after full reference
validation. Selected reads seek their metadata instead of scanning unrelated
artifacts and records. Live scans and selected seeks share one metadata collector,
and artifact/source capture and explicit reference expansion share one aggregate
catalog admission law. The existing native pinned-value decoder now admits each
caller before parsing, with one implementation for both bounded lookup paths.

Record-only selection counts retained occurrences within the captured membership;
complete parent availability and original member counts remain separate facts.
An expired whole artifact is refused even when its individual records are pinned.
A retained record can expose a reclaimed parent with unknown original metadata.
Cursors preserve the original membership and counts after pin release/expiry;
fresh reads validate current references. Artifact diagnostic/source-array scope
filters remain unfinished and are explicitly refused rather than ignored. The
artifact tool schema advertises membership only; record query filters retain
their existing shared decoder/encoder and behavior.

Focused core scenarios verify shared record-query membership, recordless parents,
overlap deduplication, selected-reference validation before filters, sparse native
seeks despite unrelated corrupt catalog entries, admission refusal with no prefix,
and frozen pages after pin release and physical catalog retirement. A real test
daemon verifies generated wire selection, CLI/MCP equal replay, scoped counts,
foreign-store/missing/released-reference and changed-membership refusals, filter
rejection and obsolete schema refusal.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (108 storage and 31
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross-workspace
gate. All 155 ownership jobs hold with zero distinct exceptions. Evidence is
`/tmp/sparrow-artifact-scope-first-check-20261007.log`.

The current client refused status, keys, sources and artifacts against a private
archived schema-25 daemon with `UNSUPPORTED`, exit 1 and zero stdout bytes.
Evidence is `/tmp/sparrow-artifact-scope-schema25-compat.as360qne`; the archive
digest is `51145f2be6894f887324d63f8d1d9c99a2d53b90aef9c3a737647cafefdc336d`.
That owned child exited cleanly under SIGINT and removed its socket.

Signed implementation `d9baf89` passed its normal full verification hook. The
owned schema-25 development unit exited successfully under SIGINT, with PID zero
and its socket removed, before the same unit/config/store reopened under schema
26 at 2026-10-08 02:43:52 UTC (observed PID 1181470). Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and no pending writes. Directory/socket permissions remain 0700/0600.

Live evidence in `/tmp/sparrow-artifact-scope-live.74p9pwyd` verifies an original
pin revision 1 capture of two records, a revision 2 expansion to three records,
stale-revision refusal before source filtering, then release of that temporary
pin. CLI/MCP continuation preserves the original two-record capture after both
mutation and release. The selected 240-byte parent reports one retained record
in scope and two original records, with complete bytes still available. Explicit
overlapping selections deduplicate counts and agree with shared scoped key
discovery. Missing/foreign/released references, changed cursor membership,
unadmitted filters and scan refusal produce no successful stdout prefix. The
temporary pin is released and the store retains zero active pins.

`/tmp/sparrow-source-wire-live.a74400mt` verifies all 16 source pages, CLI/MCP
equal continuation and scope/source/profile/watch agreement under schema 26.
`/tmp/sparrow-artifact-scope-regression.chr0mnjz` preserves default/strict
diagnostic coverage, native text/context behavior, CLI/MCP equivalence and exact
104-byte retry/export. `/tmp/sparrow-artifact-scope-raw-regression.wp5s36hf`
preserves descriptor and handoff identities, exact 82/87-byte exports, durable
removed handoff state and the producer replacement. All 195 live records remain.
Full architecture acceptance remains unmet, including artifact diagnostic
filtering, record/field/text pagination, stream generations/gaps, full governed
SPARQL surfaces and whole-corpus resource/recovery qualification.

## Schema 25 source discovery

`sourcesList`, CLI `sources STORE_ID` and MCP `sources_list` use one source
capture and one generated client/renderer. The exact intake key is `source_id`;
its separate `rdf_iri` shares the native-hash formatter used by occurrence RDF
publication. Retained complete artifacts, fragment parents and record occurrences
are counted separately. Mapping-only and configured watch-only sources remain
visible with zero retained input. Optional mapping IDs preserve absence;
watch declarations come from the daemon's bounded watch owner. Continuations
preserve captured definitions even after ingestion, evidence retirement or pin
release. Producer continuity is explicitly unknown; stream generations and a
full gap ledger remain unfinished.

The artifact eligibility walker, catalog leases, receipt wire codec, receipt
renderer and CLI catalog options each have one implementation. Source handles
are shared across store clones, bounded to eight and 8 MiB, with a 2 MiB capture,
256 KiB conservative page ceiling and an absolute 60-second lease. Capture scans
pins/artifacts/records and declared profiles under one aggregate scan/time/byte
admission. Typed status exposes source metadata handle use and ceilings.

Focused core scenarios verify exact occurrence counts, native RDF identity and
source scope matching, mapping-only and idle watch sources, fragment survival,
frozen pages after pin release/retirement, corrupt declarations and scan refusal
without a prefix. A real test daemon verifies generated wire replies, runtime
watch ownership, CLI/MCP equal continuation, later-ingestion isolation, optional
profile IDs and explicit store/schema/selector/budget refusals. The additional
source admission scenario verifies the shared eight-handle ceiling, exact
512-byte control-character intake keys, sorted watch revisions and lease expiry.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (104 storage and 30
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross-workspace
gate. All 152 ownership jobs hold with zero distinct exceptions. Evidence is
`/tmp/sparrow-source-wire-final-check-20261007.log`.

Schema-24 compatibility evidence is
`/tmp/sparrow-source-schema24-compat.hpcbmqzd`: the current client refused status,
keys, sources and artifacts with `UNSUPPORTED`, exit 1 and zero stdout bytes
against a private owned archived daemon. Its archive digest is
`d2fa1260d05c84a67e0ea1fda0a11ec7851a82fdd75593f3045a4301090a50ff`;
the child exited cleanly under SIGINT and removed its socket. There is no fallback
to the old contract.

Signed implementation `f3b9be3` passed its normal full verification hook. The owned
schema-24 development unit exited successfully under SIGINT, with PID zero and
socket removed, before the same unit/config/store reopened under schema 25 at
2026-10-08 02:01:11 UTC (observed PID 1048879). Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and no pending writes. Directory/socket permissions remain 0700/0600.

Live evidence in `/tmp/sparrow-source-wire-live.kmsu0b54` verifies all 16 source
pages, CLI/MCP equal continuation, mapping IDs matching their get operations,
exact source keys accepted by scoped key discovery and artifact listing, and
store/budget/changed-selector refusal with exit 1 and zero successful stdout.
The diagnostic source has two complete artifacts and three retained records;
the configured development watch source has one complete artifact and one
retained record, plus watch ID `development-completed`, revision 1, enabled true.
All sources explicitly report unknown producer continuity. MCP advertises 23
implemented tools and marks source discovery read-only. The captured source
handles obey the eight-handle ceiling and subsequently expire to zero.

Regression evidence in `/tmp/sparrow-source-diagnostic-regression.m5n3m2lj`
preserves default/strict diagnostic coverage, native text/context behavior,
CLI/MCP equivalence and the exact 104-byte stable retry export.
`/tmp/sparrow-source-raw-regression.jn5gofsa` preserves the descriptor and
completed handoff identities, exact 82/87-byte exports, durable removed phase
and the producer's replacement file. These checks preserve all 195 live records.
Full architecture acceptance, stream generations/gap tracking, broader
live/pin/selection pagination and governed SPARQL surfaces remain unfinished.

## Schema 24 typed artifact pages

`artifactsList` connects the shared catalog reader below to the private Cap'n
Proto daemon. CLI `artifacts STORE_ID` and MCP `artifacts_list` use one generated
client and renderer. Receipt-time selection is an explicit unfiltered/interval
union with typed `Instant` bounds. The request declares the current discovered
schema; obsolete/absent versions and foreign stores are refused before capture.
Source/cursor lengths are checked before owned copies. Status carries typed
artifact handle/byte counts, ceilings and lease duration. Pin/artifact receipt
wire encoding and agent rendering each have one implementation.

Local qualification on 2026-10-07: final `make check` passed formatting, release
Clippy with warnings denied, all workspace release tests (100 storage and 29
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross-workspace
gate. All 149 ownership jobs hold with no distinct exceptions. Evidence is
`/tmp/sparrow-artifact-wire-final-check-20261007.log`. The real-daemon scenario
verifies immutable continuation after later ingestion, CLI/MCP equal replay,
explicit unknown origin after parent reclamation, retained record counts,
strict unknown-origin receipt filtering and no successful rows on resource,
store, schema or changed-scope refusal. MCP exposes 22 implemented tools and marks
artifact listing read-only. Existing pin pages pass through the shared codecs.

A private schema 23 daemon captured from the owned development process (SHA256
`57f3a26268b16b811c79c0e68d3e47cba91018a134e3b7891a00002743f988de`)
was refused by the schema 24 CLI for status, keys and artifact listing with
`UNSUPPORTED`, exit 1 and zero stdout bytes. Evidence is
`/tmp/sparrow-artifact-wire-compat.39mlsatd`; the private child stopped cleanly on
SIGINT and removed its socket. No HTTP transport, runtime dependency, store
format or PurRDF pin changed. Full architecture acceptance remains unmet.

Verified development adoption on 2026-10-07: signed functional commit `8cd62a0`
passed the additive pre-commit gate. The owned schema 23 unit stopped cleanly on
SIGINT and removed its socket; the same unit/config/store reopened under schema
24 (observed PID 984327). Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and no pending writes. Directory/socket permissions remain 0700/0600.

Live evidence in `/tmp/sparrow-artifact-wire-live.thqjmlxm` verifies two immutable
artifact pages for `acceptance-diagnostic-20261007`, CLI/MCP equal continuation,
typed receipt-time selection, exact retained counts (three records), original
104/240-byte lengths and explicit scoped refusals. Typed status reports the one
retained catalog handle and its eight-handle ceiling. Existing default/strict
diagnostic coverage and text/context paths remain passing. Stable artifact,
descriptor and handoff retries preserve their identities and exact 104/82/87
bytes; the handoff producer replacement remains preserved. Full architecture
acceptance, further artifact scopes and source discovery remain unfinished.

## Schema 23 bounded typed discovery

`discover` carries typed store/schema/profile identities, upload/event ceilings
and bounded durability state. SDK connections and agent query admission use it
instead of scanning operational status. Discovery work does not grow with the
retained evidence catalog; lock contention can still delay it. Explicit status
retains measured catalog and physical usage. Discovery is not a health check:
corrupt evidence still fails status and affected queries with `STORAGE_FAILURE`.
The wire change leaves store formats unchanged.

Local qualification on 2026-10-07: `make check` passed formatting, release
Clippy with warnings denied, workspace release tests (95 storage and 28
RPC/daemon/MCP scenarios), census self-tests and the pinned cross-workspace
gate. All 145 ownership jobs hold, with no distinct exceptions. The shared
native catalog corruption fixture lives in one dev-only test support crate;
both dependency edges are dev-only. Store properties and status share their
immutable metadata owner; status and discovery share the durability wire codec.

A private schema 22 daemon was captured from the owned development process
before adoption (SHA256
`3108104465a2ffa04a67b7c7f3446f0780bca3183623f1182700bae1215ab909`).
The current CLI refused `status`, `keys` and `records` with `UNSUPPORTED`,
exit 1 and zero stdout bytes, without falling back to status. Evidence is in
`/tmp/sparrow-discovery-compat.2_glnuku`; the private child exited cleanly on
SIGINT and removed its socket. Full query leases/snapshots/pagination remain
unfinished.

Verified development adoption on 2026-10-07: signed functional commit `0790356`
passed the additive pre-commit gate. The owned schema 22 development unit stopped
cleanly on SIGINT and removed its socket; the same unit/config/store reopened
under schema 23 (observed PID 883173). Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and no pending writes. Directory/socket modes remain 0700/0600.

Live evidence in `/tmp/sparrow-discovery-live.p0d031bl` verifies CLI/MCP equal
proven rows and unknown coverage, incomplete native text/context coverage,
strict `INDEX_NOT_READY` refusal, and the original 104-byte artifact retry/export.
Descriptor and handoff retries retain their original identities and exact 82/87
bytes; the producer replacement remains preserved. The acceptance harness's
final record-count assertion initially compared its decimal string with an
integer; checking the documented string representation passed without another
ingest or daemon restart. Full architecture acceptance remains unmet.

## Schema 22 diagnostic scope coverage

The shared diagnostic membership owner distinguishes certain matches, certain
exclusions and unknown membership. Default JSON/text/record/context reads return
proven evidence and coverage over visited live source/time members. A known
dimension mismatch proves exclusion despite another unavailable dimension.
Otherwise missing, ambiguous or unprofiled dimensions contribute to unknown
membership. Context anchors remain strict; neighbors share default/strict policy.

Eligible and qualified totals use a typed known/unknown union; agent renderers
preserve unknown as `null` and expose known lower bounds separately. One core
codec serves all coverage receipts, including both context directions. Incomplete
membership is also propagated into event/text corpus coverage. Key counts and
scores describe the covered membership, not unknown records. Complete membership
scans retain exact totals when only output rows are limited.

`require_complete_index` is accepted across all CLI/MCP query surfaces. Applicable
JSON projection failure or undecidable diagnostic membership refuses strict
reads with `INDEX_NOT_READY`; genuine storage failures remain hard errors in
either mode. Existing record-query strict controls feed the same core policy.
No new parser, scoring law, or persisted evidence representation is introduced.

Queries now declare `Scope.schemaVersion`; the daemon refuses older/absent
versions before encoding changed count fields. Every agent query first checks
native discovery. At schema 22 that discovery still scanned status; schema 23
replaces that admission path above. Before the verified
adoption below the development unit ran schema 21.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (94 storage and 27 RPC/daemon/
MCP scenarios), seeded census self-tests and the pinned cross-workspace gate.
All 143 ownership jobs hold with no distinct exceptions. Core scenarios exercise
known mismatches with another unknown dimension, default proven rows and unknown
totals across JSON/text/combined/context reads, strict refusal, uncertain context
anchors, pin retention/reopen, complete totals with an output row limit,
incomplete traversal totals and missing captured-profile storage failures.
All five query surfaces exercise identical CLI/MCP coverage and strict refusal.
Generated requests with absent or schema-21 version declarations refuse before
query execution. Native column-family corruption fixtures share one dev-only
open helper instead of repeating the storage setup.

An independently archived schema-21 CLI (SHA-256
`1ee26e7eb898513eeab8cd3cb896a9361610628d510843b89e6a049baa93dea6`)
refused unfiltered key/text reads and a filtered key read against a private
schema-22 daemon: `UNSUPPORTED`, exit 1 and zero stdout bytes. The current CLI
returned one proven member, one unknown member, a null eligible total and a
known lower bound of one. The owned daemon shut down with SIGINT, exit 0 and
socket cleanup. Evidence is under `/tmp/sparrow-coverage-compat.btajhgee`.

The owned development unit adopted signed commit `d319902` after SIGINT shutdown,
MainPID 0, successful result and socket cleanup. PID 850878 now serves schema
22 at the same private socket, directory 0700/socket 0600, on format 16 and store
`store:46c88e949ccec658600b66c7becd5bb7`. One synthetic record with a missing
mapped severity was durably accepted as
`artifact:88c0813c1febf6bd2a103c26f669701d`, retaining all 104 original bytes.
Default combined reads through CLI/MCP agree on the existing proven worker
occurrence, one certainly excluded occurrence and one unknown member. Eligible
and qualified totals are null; their known lower bounds are one. Native text
corpus and context receipts also report incomplete membership. Strict combined
reads refuse with `INDEX_NOT_READY`, exit 1 and zero stdout. Exact-byte export
and stable ingestion retry passed. Status reported 195 live records and no
durability backlog.

Historical descriptor evidence remains unprofiled with an exact unfiltered
count of one. Its original ingestion retry and the immutable handoff retry
retained their artifact identities; independent byte comparisons passed for
all 82 descriptor bytes, all 87 handoff bytes and the replacement producer file.
Evidence is under `/tmp/sparrow-coverage-live.d53na_mw`. This is owned transient
development acceptance. Full SPARQL admission, graph correlation/explanations,
active tailing/syslog, complete read leases/pagination, permanent packaging and
whole-corpus resource/retention qualification remain unfinished.

## Schema 21 diagnostic mappings and filters

Immutable source profiles map service, host, severity and trace/span dimensions
to concrete native JSON pointers. One capture owner uses PurRDF's retained ordered
occurrences and decoded strings; timestamp interpretation shares its unique
mapped-occurrence selector. Native pointer parsing and RDF construction remain
PurRDF functions. No field name, severity case or trace syntax is inferred.
The direct and staged publication paths share the same record preparation owner.
Staged checkpoints bind profile presence or absence before the first record and
retain that decision through reopen and later source definitions.

Known dimensions retain decoded value, native occurrence, immutable profile and
original string-body span. Unknown dimensions retain explicit reasons; duplicate
fields stay ambiguous even if equal. Diagnostic graphs use independent per-record
owners and the existing dictionary/index, scoped view and retirement paths.
Inspection reports their persisted primary rows separately from metadata/content.

All query surfaces AND diagnostic dimensions with source/time/membership and OR
their bounded alternatives. Unknown filtered evidence causes `INDEX_NOT_READY`
with no successful prefix; partial diagnostic-scope coverage/counts-unknown
responses remain unfinished. CLI and MCP share mapping/filter codecs and typed
operations. Schema discovery refuses older endpoints. Source and diagnostic
alternatives now share the concrete `QueryAlternatives` admission owner.

The storage fence advances basic/consumption/watch/handoff formats to 13/14/15/16.
Legacy 9/10/11/12 retain their completed recordless index and store identity while
durably advancing before serving. Historical absent diagnostic snapshots remain
unprofiled; later mappings do not rewrite them. Legacy stores with unfenced
diagnostic registry state refuse startup. Before adoption below the development
unit ran schema 20/format 12. Full governed SPARQL,
graph-layer selection/correlation, tailing/syslog, physical reclamation and the
remaining full architecture qualification are still unfinished.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (93 storage and 27 RPC/daemon/
MCP scenarios), seeded census self-tests and the pinned cross-workspace gate.
All 140 ledger jobs hold with zero distinct exceptions. The scenarios cover
native decoded escapes and original body spans, ambiguous equal duplicate
fields, immutable staged-import absence across reopen, diagnostic RDF pin
retention and retirement, all five query surfaces through CLI/MCP, and each
legacy storage authority upgrade. Ranked combined results carry the same
snapshot as chronological results.

An independent private executable check created store
`store:7d736e9327c4ac9cf4a37831e7dc6bbe`, format 15/schema 21. The archived
schema-20 executable (SHA-256
`795b2e312a03075d0bfef5cc8341246647cc38fd8d1d1cb419489f2a7f974d2f`)
refused it with `UNSUPPORTED`, exit 1. The current executable reopened the same
identity and recovered the identical mapping and durable artifact retry receipt.
Both owned current processes shut down with SIGINT, exit 0 and socket cleanup.
Evidence is under `/tmp/sparrow-diagnostic-writer-fence.lbr4q7qy`. This verifies
writer refusal and reopen, not full architecture acceptance.

The owned development unit adopted signed commit `5f429a6` after SIGINT shutdown,
MainPID 0, successful unit result and socket cleanup. PID 802658 now serves schema
21/format 16 at the existing private socket, directory 0700/socket 0600, and store
`store:46c88e949ccec658600b66c7becd5bb7`. Historical descriptor evidence remains
explicitly unprofiled. A new synthetic source defined five mappings before
publishing two JSONL records as `artifact:a35c460d343822b6ef25bd68cb5be04f`.
Combined service/severity/trace filters returned exactly
`record:2b5f999b12f0d4e25a74fe850d832114` through CLI and MCP; native text retrieval
returned the same occurrence and RDF inspection reported 40 diagnostic rows.
The complete 240-byte export matched the input including its escape and CRLF.
Idempotent mapping/ingestion retries preserved their identities. Status reported
194 live records, no durability backlog and the same watch authority.

Original descriptor and immutable handoff retries also retained their artifact
identities. Independent byte comparisons verified all 82 descriptor bytes, all
87 handoff bytes and the preserved replacement producer file. Outputs are under
`/tmp/sparrow-diagnostic-live.nvzlvt27`. This qualifies the owned transient
development deployment; permanent packaging, full architecture acceptance and
the remaining features listed above are unfinished.

## Schema 20 source alternatives

All five query surfaces use one store-owned source predicate: OR across bounded
scope `filter.source_ids`, AND with the outer exact source, receipt interval and
live/selection/pin membership. Key/value discovery and combined/native-text
queries filter before qualification or ranking; context refuses an excluded
anchor and keeps same-source neighbors. Source spelling and occurrence identity
remain unchanged. Present empty or oversized arrays fail rather than widening
scope. The immutable `SourceFilter` type owns admission; native PurRDF remains
the JSON decoder and text/field semantic owner.

This changes the wire contract, not format-12 storage. CLI and MCP require current
native discovery for filtered requests so old daemons cannot ignore the new
field. This currently adds the existing status scan, whose scan time remains
unbounded; no bounded handshake or query snapshot is claimed. Diagnostic
normalization/filter dimensions and the remaining architecture requirements are
still unfinished.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (91 storage and 26 RPC/daemon/
MCP scenarios), seeded census self-tests and the pinned cross-workspace gate.
All 131 ledger jobs hold with no distinct exceptions. Source tests exercise OR,
duplicate alternatives, exact-source AND, half-open receipt times, pin-retained
old evidence, unavailable membership before filtering and source-local context.
The daemon integration scenario covers all five surfaces through both CLI and
MCP for live/selection/pin scopes, plus direct generated requests and invalid
array admission. A private archived schema-19 daemon independently refused a
filtered CLI read with `UNSUPPORTED`, exit 1 and zero stdout bytes. Its binary
SHA-256 is `bf9d90750fea6a1cef4ad787625d84677eaccb7936285f9a11110a0d7765fa62`;
evidence is under `/tmp/sparrow-source-filter-old.c_j3gbdi`. That owned private
daemon shut down with SIGINT, exit 0 and socket cleanup.

The owned development unit adopted signed commit `b2d751d` after graceful SIGINT
shutdown, MainPID 0, successful unit result and socket cleanup. PID 727274 now
serves schema 20 on the existing private socket and reopened the same format-12
store with 192 live records. A live scope containing `acceptance-descriptor`, an
absent source and duplicate `acceptance-descriptor` returned exactly the original
descriptor occurrence; adding an outer nonmatching source returned no rows. MCP
returned identical filtered rows to CLI. Descriptor and immutable handoff retries
kept their original identities; independent `cmp` checks passed for all 82
descriptor bytes, 87 handoff bytes and the preserved replacement producer file.
Evidence is under `/tmp/sparrow-source-filter-live.uzLVKl`. This remains transient
development acceptance, not permanent deployment or full architecture completion.

## Streaming evidence catalog reads

Status counts now stream the record and pin catalogs through one native cursor
and one decoded row at a time. They no longer allocate a vector containing all
resident records or all pin envelopes. Eligible record/artifact/pin listings and
pin-name lookup share the same iterator; explicit listing APIs still allocate
their requested result vectors. Pin lookup completes its scan and propagates
later corruption, even after finding a matching name.

The shared decoder validates catalog key/identity agreement before returning
evidence. Pin protection capture uses that same identity owner after its existing
admission checks. Malformed rows and native iterator errors remain hard failures;
filtered or counted reads never turn them into omitted rows or partial success.
Counts remain exact for the captured UTC instant under the writer lock. This
changes no wire schema, storage encoding or lifetime law.

Qualification on 2026-10-07: `make check` passed release Clippy with warnings
denied, all workspace release tests (including 89 storage scenarios and 25
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross gate.
All 129 helper ownership jobs hold with no distinct exceptions. New storage
scenarios cover exact counts across retention, pin release and reopen, and hard
failures for corrupted record/artifact/pin identities. This is local source
qualification.

The owned development unit adopted signed commit `bf0260d` after graceful SIGINT
shutdown and socket cleanup. PID 701553 reopened the same store
`store:46c88e949ccec658600b66c7becd5bb7`, format 12/schema 19, with 192 live
records, 193 payload chunks, 188 JSON projections, no pins and no durability
backlog. The socket remained mode 0600 in its mode-0700 directory. The first
connection raced socket creation; the same unit then became ready without a
second start. Descriptor ingestion and immutable handoff retries retained their
original artifact identities and durable receipts. Independent `cmp` checks
verified all 82 descriptor bytes, all 87 handoff bytes including CRLF, and the
preserved replacement producer file. The stdio MCP status adapter also passed
through the typed socket client. Captured outputs are under
`/tmp/sparrow-stream-catalog-live.J3qPAA`. This remains a transient development
unit, not permanent/reboot or corpus-scale qualification.

This removes resident catalog vectors; it does not prove a bounded count-scan
duration, bounded aggregate pin protection, corpus-scale RSS or latency. Those
requirements remain unfinished. Forced physical reclamation also remains unmet
for the concrete safe-binding gaps in
[native reclamation integration](native-reclamation.md).

## Schema 19 recordless artifact eviction

Empty JSONL and log artifacts have no addressable records but retain catalog
and retry metadata. They now publish one native `empty-time:` entry in the same
atomic batch as their artifact and acceptance receipt. Direct and staged upload
paths use one index/key owner. Ordinary metadata expiry deletes this index entry
in the same batch as its artifact; publication does not fabricate a record for
an empty input. A recordless artifact with nonzero bytes or payload references
is a hard ownership failure.

Pressure eviction merges the existing record timeline with this index through
two native peeking cursors, ordered by `(received_at, evidence_id)`. Pins protect
recordless artifacts exactly as other complete artifacts. The traversal cursor
uses the common timestamp/identity suffix; either index's earlier publication
invalidates it, and new publication after exhaustion starts directly at the new
candidate. Record queries and context continue to use their record-only index.
Each new record or recordless artifact consumes one retirement unit. Schema 19
names that frontier `unitLimit`, exposes `recordlessArtifacts` in the typed
last-pass report, and adds the session's cumulative `evictedRecordlessArtifacts`
to pressure status. CLI and stdio MCP use the same renderer.

A retired recordless artifact has no byte dependencies; its catalog and index
are removed atomically, while original acceptance/import retry receipts retain
their stable identities. Retrying the request cannot resurrect its catalog or
bytes. Native disk recovery remains separately measured and required before
further pressure retirement; catalog deletion alone does not prove space freed.

Legacy store formats 5, 6, 7 and 8 upgrade to 9, 10, 11 and 12 respectively,
preserving the existing basic/consumption/watch/handoff writer fences and watch
registry presence checks. Startup fences older writers before building the new
index. Each durable upgrade page inspects at most 512 artifact entries and
admits 32 MiB of raw/decoded catalog capture. Populated record membership is
inspected through PurRDF's native JSON event reader without materializing its
record-ID list. Complete index pages and their checkpoints publish atomically;
reopen resumes the checkpoint. Startup remains unavailable until the required
index is complete. Missing, malformed or unfenced index state fails explicitly;
unsupported writers cannot reopen an upgraded store. Upgrade does not change
artifact IDs, receipt times, content, query projections or watch authority.

Qualification covers mixed record/empty-artifact receipt order, protection and
release, direct and staged retries without resurrection, earlier publication
and publication after exhaustion, admission refusal, corrupt index rollback,
all four legacy writer-fence upgrades, durable page/checkpoint resumption after
reopen, and native duplicate-member validation with a 100,000-member populated
manifest. Real-daemon pressure acceptance retires an empty artifact and an
unpinned record while preserving pinned bytes, and compares cumulative counters
through CLI and stdio MCP. The full gate passed with 87 storage scenarios,
eight core units, three server units, 25 RPC/daemon/MCP scenarios, all remaining
workspace tests and 128 ownership jobs without distinct exceptions or
unsanctioned duplicate groups. Physical reclamation and corpus-scale RSS/latency
acceptance remain unfinished.

A separate owned private-store acceptance archived the pre-upgrade daemon
(schema 18 binary SHA-256
`0a60c01b63aa832ae364d04ea809dc399dd6df841b12c0c20985441d1f3f1d67`).
The new daemon accepted an empty input and stopped gracefully. The archived
daemon refused that store with `UNSUPPORTED` rather than opening it under the
old writer contract. The new daemon then reopened with the same store identity
and original empty-artifact receipt. Config, native store and rejection output
are under `/tmp/sparrow-recordless-writer-fence.gabyjuhl`.

At 2026-10-07 23:08 UTC, the owned development unit was gracefully cut over
to implementation commit `179f386`, main PID 615744. The existing format 8
store upgraded to format 12 and served schema 19 on the same private
`development-v5/rpc.sock`. Its persistent identity remains
`store:46c88e949ccec658600b66c7becd5bb7`; the reopened session is
`session:f0edf1fe3556a7d16580670e6e0eb8f5`. Status retained 192 live records,
193 payload chunks and 188 JSON projections, with no pins or pending durability.
Pressure remained ready with zero retirement counters. The synthetic watch
remains covered at revision 1, with no pending claims; directory/socket modes
remain 0700/0600.

Descriptor and completed-handoff retries returned their original artifact/import
identities. Independent comparisons passed for the 82-byte descriptor and
87-byte CRLF handoff exports; the replacement producer pathname remained intact.
Live stdio MCP reported schema 19, format 12, the same store and 192 live
records. Exports and MCP output are under `/tmp/sparrow-recordless-live.Pua8sL`.
This verifies actual legacy-store cutover and retained-evidence regressions,
without claiming sustained pressure, live producer intake, reboot packaging or
complete architecture acceptance.

## Bounded catalog expiry

Ordinary expiry no longer collects the artifact, record and pin corpus or
constructs one corpus-sized native write batch. `Store::expire_bounded` uses
native ordered seek over artifact, record, pin and request catalogs. Its
session-local cursor resumes complete safe prefixes, and restarts after a
completed traversal or reopen. New entries behind a position and released pins
are revisited on the next traversal. Every pass recaptures current protection
under the same serialized writer lock; progress never grants stale deletion
authority. Eligibility continues to use the current server instant, so logical
expiry does not wait for the cleanup cursor.

The existing retirement admission limits bound visited catalog entries, active
pin capture, dependency units, payload ownership references, conservative decoded
catalog shapes and final atomic write size. Complete artifact manifests drain
across passes. Individual records retire atomically through the same JSON, RDF,
text and payload owner used by pressure eviction and abandoned preparation.
An indivisible record or catalog entry that exceeds a fresh allowance fails
explicitly. Hard decode, ownership and native batch failures publish no prefix
and do not advance the cursor. Empty artifact catalogs are removed after their
ordinary metadata grace period even though they have no records or payloads.
Active consumption and handoff request receipts retain the existing ownership
law rather than being discarded as ordinary expired retries.

The daemon runs one finite pass each second; it does not hold the catalog writer
through a loop draining the corpus. `Store::expire` now invokes the same bounded
implementation with default limits and returns that pass's retirement-unit
count. Callers needing traversal evidence use the explicit frontier and
continuation in `ExpiryReport`. This changes no wire schema or store format.
Native compaction, bounded individual manifest encoding and sustained physical
reclamation qualification remain unfinished.

Qualification covers bounded manifest drainage with shared bytes and a record
pin; release and expiry of protection between pages; empty artifact cleanup;
reopen and fresh-publication traversal; atomic batch/catalog admission refusal;
and a real private-socket daemon draining 70 expired records across multiple
passes while preserving one pinned occurrence and its exact bytes. The full
gate passed with 83 storage scenarios, six core units, three server units,
25 RPC/daemon/MCP scenarios, all other workspace tests and 125 helper ownership
jobs, with no distinct exceptions or unsanctioned duplicate groups.

The initial full gate encountered the previously recorded nonblocking claim
lock failure in parallel recovery acceptance. A local native probe verified that
descriptor close can leave an exclusive `flock` held by an inherited open file
description. Completed-consumption ownership now releases the native lock
explicitly when its guard is dropped, including failures during subsequent root
initialization. Acquisition remains nonblocking and exclusive; release failures
are reported. A deterministic unit retains a duplicated descriptor across owner
release, proves immediate replacement acquisition, and proves that dropping the
old descriptor does not disturb the replacement's ownership. The subsequent
full gate passed. This verifies that lifecycle rule without claiming which
particular child inherited the descriptor in the earlier parallel failure.

At 2026-10-07 22:38 UTC, implementation commit `5b3005d` was gracefully cut
over on the owned development unit. Its main PID is 179150 and the reopened
session is `session:cc93cc2d988d7594ff1a433b132bf3da`. The same private Unix
socket serves schema 18; persistent store identity and format remain
`store:46c88e949ccec658600b66c7becd5bb7` and `sparrow.store/8`. Status reported
192 live records, 193 payload chunks, 188 JSON projections, no pins or pending
durability, ready pressure state and the covered synthetic watch at revision 1.
Directory/socket modes remain 0700/0600.

The saved descriptor and completed-handoff retries returned their original
artifact/import identities. Independent byte comparisons passed for both
82-byte descriptor and 87-byte CRLF handoff exports, and the replacement source
pathname remained intact. Live stdio MCP status reported the same store,
schema 18 and 192 live records. Exports and MCP output are under
`/tmp/sparrow-expiry-live.yV2yG5`. This proves cutover and retained-evidence
regressions; the temporary-store and owned-daemon scenarios above establish
bounded expiry behavior. It does not qualify sustained pressure, arbitrary live
producer intake, reboot packaging or complete architecture acceptance.

## Schema 18 bounded eviction traversal

Eviction retains one session-local position shared by manual core calls and the
daemon's pressure worker. Scan and catalog budgets advance past a complete safe
prefix. A partial parent manifest resumes at the same retired record until its
remaining ownership is drained. Hard storage failures and atomic batch admission
refusal leave the position unchanged. A single required catalog entry that cannot
fit still fails explicitly; it is never skipped to retire younger evidence.

Every pass captures current active pin membership through the existing pin
owner. Changed effective membership, including release and expiry, restarts at
the oldest entry. Renewal preserving the same protection continues the existing
position. Successful time-index publication is observed through RocksDB's native
borrowed write-batch visitor, without decoding a second batch format or copying
payload bodies. An insertion at or before the current position restarts traversal,
so a backwards receipt-clock step cannot hide newly accepted older evidence.
Publication after an exhausted traversal supplies the next candidate directly;
unchanged exhaustion does not repeatedly scan the same protected corpus.

The cursor holds a bounded time key and at most one admitted pin-membership set.
Each current capture remains independently admitted; retained and fresh membership
shapes are each limited by the 32 MiB conservative catalog allowance. This is
internal shape accounting, not a measured RSS guarantee. Reopening discards this
position safely and starts a fresh chronological pass.

Schema 18 exposes a `catalogBudget` logical frontier and a `continuation` boolean
in the typed last-pass report. The pressure worker stays requested when a safe
scan prefix remains, rather than declaring the corpus blocked at its first page.
Physical recovery still gates additional destructive passes after retirement.
The CLI and stdio MCP use the same generated contract and pressure renderer.

Qualification covers pressure progress through a twelve-record protected prefix
with a two-entry scan budget and renewal between passes; release/expiry exposing
an older record; earlier publication; publication after exhaustion; bounded
catalog continuation; native default-family batch observation and invalid-key
refusal; and typed catalog-frontier rendering. The full gate passed with 79
storage scenarios, six core units, two server units, 24 RPC/daemon/MCP scenarios,
all other workspace tests and 123 helper ownership jobs with no distinct
exceptions or unsanctioned duplicate groups.

At 2026-10-07 22:17 UTC, the owned development daemon was gracefully reopened
on implementation commit `e19ce63` and served schema 18 on the same private
`development-v5/rpc.sock`. Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/8`, with 192 live
records. The new session is `session:f64e79a8431598645ffb3852bddcb1d2`;
pressure status remained ready, without requested capacity or retirement.
Directory/socket modes remain 0700/0600 and the sole synthetic consume watch
remains covered at revision 1, without pending claims.

The saved descriptor retry returned `artifact:d066e0d527e9e82ec11e13388b386c13`
and its 82-byte export matched the original. The saved completed handoff again
returned its original removed import and `artifact:eec8eedea8b05f3d9512e88ff8ddd61f`;
its 87-byte CRLF export matched the source, and the replacement producer pathname
remained intact. Live stdio MCP status reported schema 18 and the same persistent
store and record count. Acceptance exports and MCP output are under
`/tmp/sparrow-continuation-live.HVXiqO`. This verifies cutover and retained-evidence
regressions, not sustained pressure or reboot/deployment qualification.

## Schema 17 pressure maintenance

Direct record ingestion and staged import writes share one measured physical
admission law. A prepared batch is refused before publication when its estimate
would cross the disk budget minus maintenance reserve. The smallest outstanding
refused estimate requests reclamation; a batch that cannot fit the entire data
allowance returns `RESOURCE_LIMIT` without requesting eviction. Buffered WAL
queue pressure and administrative control-write refusal are separate signals.

The daemon schedules one pressure pass per second, serialized with ingestion and
pin mutations. It captures native flush/compaction and snapshot observations,
uses the shared bounded oldest-unpinned retirement path, and requests nonwaiting
native flushes of both families. Automatic RocksDB compaction and blob GC use
the configured two background jobs. Missing native properties, background errors
and flush scheduling failures are actionable errors, retained in pressure status.
The worker does not manufacture a successful physical reclamation receipt.

After retirement, another destructive pass waits for native work and read
snapshots to finish and for measured physical bytes to fall below its preceding
observation. Writes continue to check their own complete estimate. Status and
pin release remain available under data pressure. A reopened over-budget store
requests maintenance from its current footprint; pressure counters and refused
estimates are session-local observations, not durable producer retry receipts.

Typed status exposes ready/requested/awaiting-reclamation/blocked phase,
requested bytes, pass and retirement totals, native activity and snapshot counts,
the last explicit logical frontier, and any maintenance failure. CLI and stdio
MCP share one renderer. Temporary-store and real-daemon acceptance use owned
ballast to force admission refusal and then remove it to prove capacity-based
retry. That does not qualify forced compaction, sustained physical reclamation,
peak RSS or the complete pressure architecture. A stalled native reclamation
cycle remains visible and backpressures writes instead of repeatedly deleting
additional evidence.

The full workspace gate passed with 75 storage scenarios, 24 real-daemon/RPC/MCP
scenarios, all SDK and other workspace tests, release Clippy with warnings denied,
seeded census self-tests and 122 helper ownership jobs with no distinct exceptions
or unsanctioned duplicate groups. This qualifies the admission/worker/status
integration described here, not the remaining full-architecture acceptance.

On 2026-10-07 at 21:55 UTC, the owned development service was gracefully reopened
on implementation commit `9f320cd`. It serves schema 17 at
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`, with directory mode
0700 and socket mode 0600. The persistent store remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/8`, with 192 live
records and no active pins. The opened session is
`session:9dba87291fed3b9bd2d81ed9cf3b6f56`; CLI and live stdio MCP status both
reported ready pressure, zero requested bytes, no evictions and no maintenance
failure. The sole configured synthetic watch remains `development-completed`
revision 1, covered, with no pending claims. This is the existing transient user
unit, not reboot/deployment qualification.

The saved descriptor request `descriptor-acceptance-20261007-01` returned its
original durable artifact `artifact:d066e0d527e9e82ec11e13388b386c13`; an 82-byte
export matched its original source exactly. Both status and admission retry of
the saved completed-file handoff `handoff-acceptance-20261007-01` returned the
original removed import and artifact `artifact:eec8eedea8b05f3d9512e88ff8ddd61f`.
Its 87-byte CRLF export matched the original source, and the replacement producer
pathname still contained `replacement preserved after handoff` plus newline.
Exports and live MCP messages are under `/tmp/sparrow-pressure-live.vQGn2o`.
No pressure ballast or live producer intake was added to this development store.

The initial workspace run hit a transient `WouldBlock` on the existing claim
recovery fixture's coordination lock, alongside the corrected MCP fixture
assertion. The final workspace gate and mandatory commit hook both passed that
recovery fixture. This change does not claim to explain or fix the transient
lock refusal. Logs are `/tmp/sparrow-pressure-check-20261007.log`,
`/tmp/sparrow-pressure-final-check-20261007.log` and
`/tmp/sparrow-pressure-commit-20261007.log`.

## Typed explicit completed handoff

Schema 16 adds `watchHandoff` and read-only `watchHandoffGet`. The Rust SDK and
CLI use the same generated client and receipt certification. The request binds
the original persistent store, stable request ID, registered watch revision,
relative Unix path bytes and exact device/inode/length/mtime/ctime generation.
First admission requires the current enabled finished-file watch and an anchored
regular-file observation, with file and parent synchronization. Explicit producer
handoff bypasses the discovery quiet timer; it does not weaken generation checks.

Admission uses the existing bounded worker control queue and atomic storage
reservation. It does not hold a publication RPC semaphore while awaiting the
worker. Every queued catalog mutation refreshes pending ownership, including
handoffs admitted earlier in that same tick. The same worker then performs
atomic private claim, SDK upload, durable publication and synchronized removal.
An admission receipt says `reserved`; progress can advance through `claimed`,
`published` and `removed`. Admission does not certify publication or deletion.
No separate importer, deletion path, parser or retry transaction is introduced.

The CLI workflow is:

```sh
sparrow SOCKET watch handoff-prepare REQUEST WATCH REVISION RELATIVE_PATH INTENT.capnp
sparrow SOCKET watch handoff INTENT.capnp
sparrow SOCKET watch handoff-status INTENT.capnp
```

Preparation writes one mode-0600, synchronized, create-new Cap'n Proto intent
outside enabled consumption roots. It does not reserve, rename or delete the
source. Intents are bounded to 16 KiB and reject trailing messages, symlinks and
special files. Retry `handoff` with that saved intent; do not run preparation
again against a replacement generation. The SDK supports non-UTF8 source paths.
Stable retries recover their original binding before touching current roots,
definitions or source paths, including after watch removal and daemon restart.
The default stdio MCP search surface receives no destructive handoff authority.

The SDK codec unit module owns one dev-only intent fixture shared by its two
adversarial tests, recorded with the typed-intent ledger job and vectors. The
existing census excludes `cfg(test)` modules, so that fixture is verified by the
unit checks rather than asserted as an independently indexed shipping symbol.
No detector, exception or index rule was relaxed.

The final-source `make check` gate passed with 12 SDK unit tests, 68 storage
scenarios, 23 real-daemon/filesystem/MCP scenarios, all remaining workspace
checks, release Clippy with warnings denied, seeded census self-tests and 114
ledger jobs. The real-daemon handoff scenario covers missing/stale/foreign
authority, generation/path conflicts without admission, immutable CLI preparation
and unsafe output refusal, quiet-timer bypass, exact CRLF bytes, raw non-UTF8
filenames, original retries after replacement/removal/restart, and no duplicate
records. These checks qualify explicit handoff; full architecture acceptance,
active tailing, syslog and the other outstanding query/operational work remain
unfinished.

The owned development daemon was updated from signed implementation `7aea5fc`
and reopened successfully after the live acceptance. It serves schema 16 at
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`, preserving store
`store:46c88e949ccec658600b66c7becd5bb7` and now fencing writers with
`sparrow.store/8`. Directory/socket modes remain 0700/0600. Captured status shows
192 live records, no pins, no pending durability writes and no watch failure.
The current daemon is the owned transient user unit `sparrow-development.service`;
this is not reboot-persistent packaging or full crash-lifecycle qualification.

The live synthetic watch `development-handoff` was created at revision 5 with a
600,000 ms quiet interval and source `development-handoff`. Its mode-0600 intent
`/tmp/sparrow-handoff-live.b62DhU/intent.capnp` binds request
`handoff-acceptance-20261007-01`. Explicit handoff advanced from durable `reserved`
to `removed` without waiting for that quiet interval, preserving import
`import:10275c3eb8af097970ac07ac615480a416e5242f58488680fdea93de4641b71a`,
artifact `artifact:eec8eedea8b05f3d9512e88ff8ddd61f`, and record
`record:380ba5359ea12540f996eef35cd723ac`. Exact export matched the complete
87-byte CRLF input saved as `/tmp/sparrow-handoff-live.b62DhU/expected.jsonl`.
Value and full-text searches each returned this record with complete coverage.
An event interval from `2026-10-07T21:00:00Z` to `21:01:00Z` returned the one
known event with zero unknown membership, using captured profile
`time-profile:d05caa4278d528afb294b4eee05255daa042b0ab818ba5f451b2f5350a63e2fb`.

The watch was removed at revision 6 after publication. Retrying the original
intent before and after a graceful daemon reopen returned the identical import,
artifact and `removed` phase; a replacement at the original source path remained
intact at
`/home/paudley/.local/state/sparrow/development-handoff-acceptance/finished.jsonl`.
The live count remained 192; no retry allocated another occurrence. The earlier
ordinary descriptor-import retry still returned original artifact
`artifact:d066e0d527e9e82ec11e13388b386c13` and record
`record:23febe08e38e42deea0f58e4880b9358`. Current admission contains only the
original `development-completed` revision-1 synthetic watch. No live producer
directory is configured, and no sibling/model service was modified.

## Explicit handoff storage foundation

Core storage now binds a caller-owned `ConsumptionHandoff` request ID, exact
watch revision, raw relative Unix path and observed device/inode/length/content
clocks to the existing consumption journal. Reservation and retry binding share
one durable write batch. Discovery and explicit admission use one reservation
implementation, path validator and native PurRDF catalog codec. A binding proves
admission only; publication and deletion still require the existing claim,
upload, durable publication and synchronized removal path.

There are at most 128 retained handoff bindings and 16 active journals. Retrying
unchanged arguments recovers original ownership without inspecting a current
source path; conflicting revisions, paths or generations fail. Active bindings
survive ordinary receipt expiry. Removal confirmation atomically starts their
48-hour completed retry window. Missing active ownership is a hard storage
failure, including during expiry. A journal does not pin retained evidence.

First handoff admission requires a durable watch catalog and fences older
writers with `sparrow.store/8`. Subsequent catalog replacements preserve that
marker. The storage API relies on the server to verify the actual registered
revision, enabled state, explicit producer authority and descriptor generation
before first admission. Schema 16 wires those checks through the worker and typed
operations described above.

The storage foundation commit `fad05de` passed the full `make check` gate with
68 storage scenarios, 22 real-daemon,
filesystem and MCP scenarios, all remaining workspace checks, release Clippy
with warnings denied, seeded census self-tests and 109 ledger jobs. The extended
consumption lifecycle covers restart, original publication beyond retry and
retention, removal-window renewal and completed expiry. The admission scenario
covers missing catalog and overflow refusal without orphan reservations,
revision/path/generation conflicts, bounded bindings, reopen and hard failure
after deliberate active-journal corruption. These are storage and regression
checks; they do not qualify a live explicit handoff operation.

## Durable typed watch administration

Schema 15 adds `watchList`, `watchPut`, `watchRemove`, durable definitions/revisions
in status, and explicit CLI administrative commands. `watch put REQUEST_ID 0
DEFINITION.toml` creates a missing definition; the same command with its current
revision updates it. `watch remove REQUEST_ID WATCH_ID CURRENT_REVISION` disables
new source admission. The definition file uses the same strict `ConsumeWatch`
fields as one startup entry, without the `[[consume]]` table wrapper. Its explicit
finished-file producer contract authorizes private-source retirement after the
existing durable publication boundary. Ordinary MCP search tools gain no watch
mutation authority.

The one worker owns mutations through a 16-command typed channel. Administrative
RPC waiting does not hold the publication semaphore, which would prevent the
worker from completing its own typed upload. Store-scoped mutations compare the
current watch revision, carry stable request IDs, and return durable receipts.
The native framed BLAKE3 identity covers operation, exact arguments and expected
revision. A retry returns its original immutable receipt even after later
updates/removal; it does not restore that old authority. Current list/status,
rather than a historical retry acknowledgment, describes current admission.

The server owns one strict version-1 TOML catalog codec. Core storage treats its
bytes as opaque, bounds the complete catalog at 512 KiB, and commits its exact
capture compare-and-set and format marker in one durable write batch. There is
no second configuration parser or filesystem sidecar transaction. The catalog
binds to the persistent store and contains at most eight active/draining roots
and 128 bounded mutation receipts, with eight slots reserved for removal rather
than further put/update requests. Receipt expiry is 48 hours. Capacity refusal
preserves the old catalog; an uncertain persistence or invariant failure is
terminal, so the worker cannot continue with stale source authority.

Initialization atomically fences old writers with `sparrow.store/7`. New writers
accept prior formats; their first registry publication upgrades it. A later
consumption reservation cannot downgrade that marker. A format/catalog mismatch
or malformed stored catalog fails actionably rather than use startup defaults.
Startup configuration is bootstrap-only once the catalog exists: changing or
restoring an old `[[consume]]` block does not undo durable administrative removal.
Structural configuration errors remain errors, while an obsolete bootstrap root
need not exist after its durable definition has been removed.

Root admission reuses the same strict configuration checks, no-symlink
`ClaimRoot`, private lock and generation-bound journal machinery. A new root is
opened before publishing its authority; no claim is admitted before catalog
commit. Updating the same root starts fresh readiness evidence. A different
physical root is refused while any old journal remains. Removal clears discovery
and candidates, retains the disabled definition and held root for recovery, and
automatically retires that definition after all persistent claims finish. It
does not cancel staging, delete unexpected bytes or abandon a blocked claim.
Removed roots with no pending journal stop immediately. The worker rotates root
admission after a processed claim instead of always prioritizing the first root;
sustained multi-root fairness/throughput still needs measurement.

Tests exercise real-daemon create/update/list/remove, explicit authority refusal,
foreign-store and stale-revision refusal, changed retry arguments, ordinary
ingestion after dynamic registration, immutable retries across restart, and stale
bootstrap configuration after removal. A removed watch with a mutated private
claim stays disabled and recoverable across restart, preserves new unclaimed
files and refuses rebinding before touching the other root. Corrupt durable
catalog startup is a hard failure. A real-daemon pressure fixture fills all 120
put receipt slots, verifies refusal, and still disables the watch using reserved
removal capacity. Core coverage proves exact catalog CAS,
capacity refusal, persistent identity/reopen and preservation of the format-7
fence during later source reservation.

The qualification gate includes release Clippy with warnings denied, ten SDK
units, five core units, 67 storage scenarios, 50 census units, five census gate
fixtures, one server unit, 22 RPC/filesystem/MCP scenarios, doctests and the seeded
self-test. The pinned cross gate checks 105 ownership jobs without detector
relaxation or distinct exceptions. The ordinary signed-commit hook runs `make
check` on final source. Live development acceptance is recorded separately.

Explicit completed handoff, active tailing, forced kernel overflow and watch
exhaustion, failure injection at every process/crash boundary, full governed
SPARQL and the remaining full architecture/pressure/memory/deployment acceptance
are still unfinished. Administrative catalog publication does not establish
those results.

The signed implementation `c7adbbc` passed its final ordinary pre-commit gate
on 2026-10-07, including all 22 RPC/filesystem/MCP scenarios and the 105-job
pinned census. The previous owned development PID 2566561 exited successfully
on SIGINT and removed its endpoint. The updated unit is active as PID 3067453
at the same mode-0600 Unix socket, with mode-0700 data and the original store
`store:46c88e949ccec658600b66c7becd5bb7`. Its first administrative catalog commit
upgraded the marker to `sparrow.store/7`; discovery reports schema 15. Existing
190 records were present before this acceptance input.

Live typed administration created the owned synthetic root
`/home/paudley/.local/state/sparrow/development-admin-acceptance` at revision 2,
updated its quiet interval from one to three seconds at revision 3, and later
removed it at revision 4. Its exact 90-byte CRLF JSONL input was consumed as
`artifact:f63d6185e7f7a6abaf49b34f0dc6970c`, record
`record:bd6303bb090e0faf83f4050d3a8a7ba1`. Native byte retrieval compared equal to
the original input; value searching and the declared `/at` RFC3339 event interval
returned one exact row with complete membership and zero unknown events. Its
immutable time profile is
`time-profile:5ed7182c60ad536eea8a18d827ddb592dd263c19af4b0a284d2754d6816fb2eb`.

Retrying `watch-admin-create-20261007-01` with its original definition returned
the original revision-2 receipt after removal, while current list/status still
contained only `development-completed`, revision 1. A new file published after
removal remained untouched beyond the removed watch's quiet interval. The
final observed status showed 191 live records, no pending claims, no watch or
durability failure, and no pending durability writes. Retrying the earlier
ordinary descriptor fixture retained
`artifact:d066e0d527e9e82ec11e13388b386c13` and
`record:23febe08e38e42deea0f58e4880b9358`.
Only owned synthetic roots were administered. No live producer/model directory
or sibling service was changed; permanent deployment and the full architecture
remain unqualified.

## Configured recursive completed consumption (schema 14 qualification)

Schema 14 adds typed `Status.consumeWatches`; the daemon now starts one owned
Linux consumption worker for explicit `[[consume]]` roots. The development
configuration has only the owned synthetic intake described below. An example for a directory that the
operator explicitly owns and authorizes Sparrow to consume is:

```toml
[[consume]]
id = "completed-application-dumps"
root = "/srv/application/completed"
producer_contract = "finished_files"
source = "application-dumps"
format = "jsonl"
quiet_ms = 30000
max_directories = 1024
max_candidates = 4096
```

The contract is required: producers hand off finished regular files and never
write them afterward. Quiet time is an observation within that contract, not
a writer lock. Ordinary imports remain non-destructive. Configuration rejects
overlapping roots, unknown fields, missing contracts, unbounded capacities and
incompatible format names. Root admission rejects symlink components and overlap
with Sparrow's data directory. At most eight roots are admitted, with at least
two RPC connections so the owned SDK connection can coexist with callers.

Each recursive directory is opened with the existing anchored `openat2` owner
and watched before it is scanned. Linux inotify has no dirfd registration API;
the only followed alias is the kernel-owned `/proc/self/fd/<held-directory>`.
Candidate metadata also uses that held-parent alias and `symlink_metadata`,
without opening the candidate or generating a self-induced `IN_OPEN`. The SDK's
`CompletedFileIdentity::capture` remains the sole generation observation owner.
Symlinks, FIFOs, devices and sockets are excluded. Native `statat` handles unknown
directory-entry types without assuming that they are regular files.

Scans advance by 64 entries per 50 ms worker tick. Candidate and rename maps,
directory descriptors and scan queues have explicit capacities. Candidate
saturation preserves excess files, exposes `deferred`, and schedules a new scan
as capacity becomes available. The scanner can finish coverage while candidate
capacity is full, so a full candidate registry does not prevent draining its
admitted prefix. Periodic reconciliation begins after complete scan coverage,
at the larger of 60 seconds or twice the quiet interval plus one second; it
cannot continually restart an unfinished large scan.

Open/write/attribute/creation/close-write/publication events reset quiet time.
Read-only closes are not completion signals. Rename cookies reconcile candidate
movement, with inode identity still checked at the destination. Overflow,
ignored/unmounted directories, directory moves and recursive namespace changes
start a new inotify descriptor epoch and clear stale readiness. An event drain
is bounded at 4096 events; if that limit can discard prefetched Reader bytes,
the entire coverage epoch is invalidated before further claims. No claim is
admitted during reconciliation, and completion of coverage starts fresh quiet
timers. Directory/root/lock coverage failures are actionable terminal daemon
errors; they do not authorize source retirement. The daemon joins the worker,
flushes storage and removes its owned endpoint on an orderly terminal path.

The worker resumes the existing bounded persistent journal registry before
admitting new files. Recovery, SDK upload and private-source unlink share the
previously qualified `ClaimRoot` implementations. RPC discovery has a finite
10-second deadline and claim publication a 45-second deadline. Failure retains
the original journal/request identity and retries after a five-second delay;
it never substitutes a second source occurrence. Pending journals cannot be
silently abandoned by deleting their watch from configuration. Shutdown stops
admission and cancels asynchronous waiting while preserving recoverable claims.
It joins while the server LocalSet still drives its owned Cap'n Proto connection.

Typed status exposes configured ID/root, reconciliation or complete coverage,
held directories, tracked candidates, deferred inputs, pending/completed claims,
coverage epochs/losses and the latest bounded unresolved failure. CLI and stdio
MCP use the same generated reply and renderer; no HTTP interface was added.
The four new acceptance fixtures cover strict configuration, actual recursive
daemon consumption with a one-candidate limit, original-byte queries, writable
reopen deferral, newly created directories, later source generations, confirmed
journal recovery, late-write preservation, directory coverage limits, root
replacement and consumer shutdown. Samples and sibling services remain untouched.

Qualification uses the normal `make check` gate: release Clippy with warnings
denied, ten SDK units, five core units, 66 storage scenarios, 50 census units,
five census gate fixtures, one server unit, 18 RPC/filesystem/MCP scenarios,
doctests and the seeded census self-test. The pinned cross-workspace gate checks
95 helper ownership jobs with zero distinct exceptions or unsanctioned groups.
The signed commit's ordinary pre-commit hook runs the same gate on final source.

At this schema-14 qualification, dynamic typed watch administration, explicit completion RPC, forced kernel
overflow and watch-exhaustion injection, whole-process crash injection at every
claim boundary, fairness across continuously busy roots, active-log tailing and
whole-corpus memory/throughput acceptance remained unfinished. No reboot-persistent
service or production producer handoff has been qualified by this change.

The signed implementation `5126d8b` passed its ordinary final pre-commit gate on
2026-10-07. The previous owned development unit, PID 2481486, exited successfully
on SIGINT and removed its socket. The updated unit is active as PID 2566561,
serving the same private `development-v5/rpc.sock` and persistent store
`store:46c88e949ccec658600b66c7becd5bb7`. Its configuration now authorizes only
the owned mode-0700 synthetic directory
`/home/paudley/.local/state/sparrow/development-completed`, with a 30-second quiet
interval, 16 directory watches and 64 candidates. The excluded private claim
directory is also mode 0700; the RPC socket is mode 0600.

A synthetic 82-byte CRLF JSONL input was atomically published into that root.
The live worker durably consumed it as
`artifact:751286900f498e0c4a3cf272fce7376d`, record
`record:bccb3dfc0bea556833fba8f475b0fb2f`, and retired only its private claim.
CLI byte retrieval compared equal to all original bytes, including the CRLF.
Native value searching returned the exact message; event-time searching for
`2026-10-07T20:00:00Z` through `20:01:00Z` returned its known source instant with
zero unknown events and complete membership. The explicitly defined `/at`
RFC3339 profile is
`time-profile:4923153986ea40fed2adcd0e58d54f57fd04c2fea3e608cb294cef8fab62cdf1`.

The first live consumption reservation upgraded the store marker to
`sparrow.store/6`. Status reported schema 14, 190 live records, one completed
claim, no pending claims, no failure and no pending durability writes. Retrying
the previous independent descriptor fixture retained
`artifact:d066e0d527e9e82ec11e13388b386c13` and
`record:23febe08e38e42deea0f58e4880b9358`. No live application/Brainstem directory
was configured or consumed, and no sibling/model service was changed. This is
live development acceptance; the full architecture and permanent deployment
remain unqualified.

## Transport and present operations

The daemon uses **Cap'n Proto RPC over a private Unix-domain socket**. The HTTP
implementation and its dependency were removed following the transport decision
on 2026-10-06. No JSON operation envelope is sent between clients and the daemon.
The generated typed contract is
[`sparrow.capnp`](../crates/sparrow-client/schema/sparrow.capnp). Domain results
and failures are explicit unions; counts and clock instants are unsigned integer
fields, formats and JSON kinds are enums, and references carry a store identity.

Present RPC methods include the eight import methods documented below,
plus `status`, `flush`, `ingest`, `eventsIngest`, `artifactGet`, `recordGet`,
`bytesRead`, `pin`, `pinRenew`, `pinRelease`, `pinRead`, `keysList`, `valuesSearch`, `textSearch`, `recordsSearch`, `recordsContext`, `rdfInspect`, `sourceTimeDefine` and `sourceTimeGet`. The shared typed
client transport is `sparrow_client::connect`, used within a Tokio `LocalSet`.
The `sparrow` CLI and stdio MCP adapter share generated-client operations and
result rendering. The CLI supplies status, non-destructive resumable file import,
exact byte export, key/value and full-text searches. MCP also supplies artifact/
record metadata and pin/release calls. CLI and MCP both supply stable-ID pin renewal
and paginated pin discovery/member retrieval. Remaining CLI commands and query surfaces
are required work.

## Standalone typed Rust producer client

`crates/sparrow-client` owns the schema compilation, Unix transport, discovery
guard, typed artifact receipts and completed-file upload implementation. The
daemon imports the generated contract from this crate. The CLI uses its public
`import_file` function; there is one hashing, file-identity and resumable upload
path. The existing server exports are reexports of these same generated types.

`sparrow_client::Client::connect` checks discovery before producer mutations.
Within a Tokio `LocalSet`, callers can use `ingest`, `events`, `import_file`, `import_open_file`, `artifact`,
`discovery`, and the generated query capability. Direct ingestion rejects inputs
above the discovered buffered-input limit; file imports hash and upload through
one 64 KiB buffer. Both return owned `ArtifactReceipt` values. Reuse the original
request ID and immutable bytes after a connection failure. Domain errors retain
their exact daemon code, message and retryability; incompatible discovery, I/O,
protocol and transport failures remain explicit.

The producer dependency graph was checked with `cargo tree -p sparrow-client -e
normal`: it contains the Cap'n Proto/Tokio transport, native PurRDF hashing/JSON
and safe Rustix filesystem calls,
without RocksDB, `sparrow-core`, or the semantic engine. Release acceptance now
uses the SDK to recover a partially prepared oversized record after a daemon
crash, verifies the same receipt through CLI retries and SDK inspection, then
checks durable direct ingestion and conflicting request refusal. Discovery tests
refuse old schema versions and missing projection identity. The Rust example in
the crate documentation is compiled as a doctest.

The direct artifact/event-batch client and bounded `Delivery` worker are present.
The worker reconnects with immutable pending requests and returns durable
admitted-input fences through flush/shutdown. The composable `TracingLayer` now
captures bounded typed fields and span callbacks through that same worker.
Schema 12 below adds explicit buffered server acknowledgments and the shared
WAL flush contract. Full server acceptance remains unmet.

### Schema 11: typed producer event batches

`eventsIngest` accepts at most 256 events, a conservative 1 MiB batch expansion,
64 KiB per event, 128 fields per event/span, and 32 context spans. Every string
is at most 16 KiB. Shared SDK admission runs both before encoding and during
daemon decoding; counts and quotas are checked before list/string allocation.
Discovery advertises batch limits. The client refuses older schemas or different
batch bounds before producer mutation.

`ProducerEvent` carries a nonzero increasing client sequence, producer UTC Unix
milliseconds, severity/kind enums, metadata name/target, ordered typed fields,
span context, and optional subject/follows-from identities. Sequence gaps are
allowed and remain visible; this operation does not promise a complete sequence
history. Producers must use a fresh 16-byte client instance for each process and
preserve the exact pending batch and request ID across reconnects. Server retry
identity is the batch request ID, not a global deduplication of span/sequence IDs.

The daemon emits canonical native JSONL using PurRDF's writer and native integer/
floating codecs, then uses the existing atomic record/dependency builder and
durable receipt law. The stored source is this explicit structured-event
representation, not a claim of preserving an unseen textual producer log. Each
event is one independently retained occurrence. Field wrappers retain Rust kind
alongside queryable values. Floating fields always retain all 64 bits, including
negative zero and NaN payloads; nonfinite values have an explicit classification
and no fabricated JSON numeric value. Binary fields retain exact native hex.
Debug/error renderings keep their explicit kinds. Span snapshots/lifecycle and
follows-from references are source evidence; a separate normalized diagnostic
RDF relationship view is still required.

Typed events declare `/event_time` with Unix-millisecond interpretation through
the existing immutable source profile operation before artifact ingestion. An
incompatible existing source profile refuses the batch. A new profile may remain
if subsequent artifact admission fails; no event prefix or acceptance receipt is
published on that failure. Acceptance/expiry clocks remain server clocks.

Release acceptance verifies all field variants, repeated names, escaped text,
span context, exact timestamp evidence spans, temporal/text retrieval, unchanged
receipts after restart, changed NaN-payload conflict, and oversized/invalid input
refusal. The `Delivery` worker below supplies automatic batching/reconnection,
drop counters and durable admitted-input fences. Tracing callbacks are implemented
below; schema 12 also provides explicit buffered server writes and WAL fences.

The owned development daemon was replaced with this qualified schema-11 build
on 2026-10-07. It retained store
`store:46c88e949ccec658600b66c7becd5bb7`, store format `sparrow.store/5`, and the
existing private socket. PID at verification was `1407618`. A live call through
the standalone producer example accepted one 380-byte event as
`artifact:e6c56d2df4de85a31e00c2a01cb4fda9` with record
`record:be55d651c34cd9c68b389dae6f9b0511`; identical arguments returned the same
durable receipt. Temporal/text retrieval found exactly that record at producer
time `2023-11-14T22:13:20.002Z`, boolean value search returned original `true`
source bytes, and a complete byte export reproduced the declared event source.
The source was synthetic; production sample files were untouched.

```sh
CARGO_BUILD_JOBS=4 cargo build --locked --release --workspace --bins --examples
target/release/examples/events /home/paudley/.local/state/sparrow/development-v5/rpc.sock acceptance-schema11-events schema11-typed-event-v1 00112233445566778899aabbccddeeff 1700000000002 'allocation failed typed acceptance'
```

The example takes an explicit instance identity and timestamp so a retry can
reproduce the same input. A new producer process must choose a fresh instance
for new observations; this direct event example remains separate from the
application tracing example below.

### Bounded producer delivery worker

`sparrow_client::Delivery` owns one producer thread, a Tokio LocalSet and the
shared typed client. Producers call `try_send(ProducerEvent)` without waiting for
socket I/O or a queue lock. The worker assigns increasing sequences under queue
serialization. Full, contended, invalid, closed and failed admission each have
visible drop counters. A full-queue refusal can leave an explicit sequence gap;
refusals before sequence assignment remain visible through counters. Event
admission also charges unused string/vector capacity, preventing a tiny value
from retaining an oversized allocation. Caller-owned inputs must already be
constructed; the tracing layer below bounds capture before queue admission.

Defaults are 1024 queued/in-flight events, 4 MiB conservative event charge,
128 events/512 KiB per batch, two-second intended batching, five-second RPC
deadlines and 250 ms retry delay. Setup validates finite upper/lower bounds;
these are configurable. Queue and immutable pending-batch ownership stay charged
until a matching durable receipt. Retryable Unix-socket/transport/daemon errors
preserve exactly the same request ID, instance, sequences, timestamps and values.
RPC timeout means delivery unknown. The first discovered persistent store binds
the worker; reconnecting to another store fails explicitly with pending input
retained. Permanent failures remain inspectable through `stats` and
`pending_batch`, with no manufactured acknowledgement. Source-time registration
pressure now uses the same retryable `STORAGE_PRESSURE` code as ingestion.

`stats` exposes offered/admitted/durable counts, drops by reason, pending count/
charge, in-flight count, retry count, terminal/transient failures, oldest pending
age and flush lag. Charges bound retained work; they do not measure process RSS,
native transport arenas or allocator overhead. The queue's reserved container
capacity remains bounded by setup limits even after it drains.

`flush(timeout)` forces an admitted-sequence fence. `shutdown(timeout)` also
closes admission and waits for the worker/socket to finish. One try-acquired
control slot rejects simultaneous barriers as `Busy`; there is no unbounded
control-message queue. Watermarks carry instance, required/durable sequence,
durable event count, pre-fence drops and the last store-scoped receipt. If any
offer was dropped before the captured fence, the barrier returns `Dropped` with
the admitted-input watermark instead of certifying all offers. A timeout retains
pending work and may be retried. A timed-out shutdown remains closed. Offers
that finish after fence capture belong to a later fence. Dropping the last
handle cancels the owned worker; it does not promise a flush or producer-process
survival. Empty shutdown returns no fabricated receipt.

This is durable delivery for this producer's admitted events. It does not expose
a server-wide flush; schema 12 supplies a separate accepted-storage fence.
Every current event batch receipt must be `durable` before queue retirement.

Release scenarios use real temporary daemons and a typed forwarding relay that
withholds a real durable reply. They verify identical retry bodies/receipt and
one occurrence, count/byte bounds during daemon absence, unchanged pending work
after timeout, explicit drops after recovery, timer-driven delivery without a
flush, fresh process instances, store replacement refusal, reserved-allocation
admission, and cancellation of an unresponsive owned socket before its 60-second
RPC deadline. Production and test relay connections now share the same bounded
server transport constructor. No model/Brainstem service lifecycle is involved.

```sh
CARGO_BUILD_JOBS=4 cargo build --locked --release --workspace --bins --examples
target/release/examples/delivery /home/paudley/.local/state/sparrow/development-v5/rpc.sock delivery-example 1700000000002 'bounded delivery example'
```

Live verification on 2026-10-07 replaced only the owned development daemon with
the qualified build, preserving store identity and schema 11. PID at verification
was `1642510`. Two separate runs of the public delivery example with identical
message/source/time produced fresh client instances
`56799eb3eeea574d8eac6d652e27270b` and `eba3448b332f3ff224a74dfc00386e37`, with
durable sequence-1 receipts for independent artifacts
`artifact:36d39d40186e4eba4efaf62aebff8500` and
`artifact:fb90b17321830cad8aa9527545ba6fa3`. Native temporal/full-text retrieval
returned both 341-byte occurrences at producer time
`2023-11-14T22:13:20.003Z`, with zero unknown time or failed text coverage. Lost-
reply retry qualification remains the deterministic real-daemon relay scenario,
where an unchanged instance/request returned the same occurrence. These synthetic
events do not establish production throughput, RSS or complete tracing acceptance.

```sh
CARGO_BUILD_JOBS=4 cargo build --locked --release -p sparrow-server
target/release/sparrowd --config config/development.toml
target/release/sparrow /home/paudley/.local/state/sparrow/development-v5/rpc.sock status
```

### Composable Rust tracing capture

The standalone client now provides `TracingLayer::new(delivery.clone())` for
application-owned `Registry`/`LookupSpan` subscriber composition. It installs no
global subscriber. All callbacks feed the existing delivery worker and its
immutable retry identities; no second transport, queue or worker is introduced.
Use the production example with a synthetic source:

```sh
cargo run --locked --release -p sparrow-client --example tracing -- \
  /home/paudley/.local/state/sparrow/development-v5/rpc.sock acceptance-tracing-layer
```

Capture preserves all shared primitive field types, including exact float bits,
binary and 128-bit integers. Debug/error formatting has explicit wire variants.
Span new/update/enter/exit/close/follows-from observations include bounded
root-to-subject snapshots, explicit parent bindings and the latest valid fields.
At most 1024 live snapshots are retained, each conservatively charged at 16 KiB;
text/binary/debug/error values refuse above 4 KiB, fields above 128 or 16 KiB,
and context above 32 spans or the shared 64 KiB event admission bound. The layer
walks ancestry leaf-first and charges before cloning to avoid unbounded scope
collection. Closed spans release owned snapshots and admission slots.

Capture failure increments the shared Invalid drop counter, including invalid
clock, unavailable context, formatting/field bounds and span state contention.
A failed span update invalidates future context capture rather than emitting
stale fields. Drops remain explicit errors at flush/shutdown. Subscribers that
remap span IDs are refused visibly. User formatters and the application's own
subscriber internals are outside the layer's allocation/computation control.

The real-daemon scenario exercises all wire field variants, nested/explicit-root
context, updated fields, all lifecycle kinds and store occurrence counts against
the admitted/drop fence. Client scenarios exercise sticky formatting refusal,
failed-update invalidation, reclamation of the 1024-span admission capacity and
unique producer identities across independent subscriber registries sharing one
worker. Direct typed span producers sharing it use `Delivery::next_span_id` too.
These checks do not establish sustained throughput or process RSS. Schema 12
adds buffered/WAL-flush controls; normalized diagnostic RDF relationships remain
unfinished.

The full release-profile `make check` passed: eight client unit tests, 56 core
storage scenarios, eight real-daemon/MCP scenarios, the other workspace checks,
Clippy with warnings denied and the seeded/pinned helper census. All 59 ledger
jobs hold with zero distinct exceptions or unsanctioned isomorphic groups.

Live application-example acceptance on the existing schema-11 development
daemon delivered five durable observations with zero drops, producer instance
`00e9e0542ab8704de7917857bac03658`, artifact
`artifact:b9131864191c576a5782ef44d7b9cc9e` (2014 bytes). Native text search for
`structured`, field value `/fields/1/value` of kind `true`, and the producer-time
interval `[2026-10-07T16:42:42.365Z, 2026-10-07T16:42:42.366Z)` each recovered log
record `record:d04d979eee5329d44da61b9f138c5ca5`, with original field-byte evidence
and zero unknown time or failed text coverage. The daemon remained at the same
store identity and private socket; no sibling producer or service was changed.

### Schema 12: explicit buffered acceptance and WAL fences

Direct SDK artifact/event ingestion can request `Durability::Buffered` through
`ingest_with_durability` / `events_with_durability`; existing default methods and
the tracing delivery worker still require durable acknowledgment. Buffered
acceptance uses the same atomic catalog, payload and native-index publication
with WAL enabled. Its receipt carries a `buffered_through` store/session/sequence
fence. `Client::flush(&fence)`, CLI `flush STORE_ID SESSION_ID SEQUENCE` and MCP
`flush` route through one typed SDK helper. Inspection/retry upgrades durability
without replacing occurrence IDs; mode changes do not change request identity.

The server ledger refuses a new buffered write before publication at 256 pending
writes or 64 MiB of atomic batch byte charge. Repeated requests consume no new
ledger entry. Successful synchronized writes/explicit WAL sync retire earlier
pending charges. Empty control batches use an explicit sync rather than treating
an empty write as a boundary. Write/WAL-sync failure latches uncertainty and
refuses receipt certification until the store is reopened. Native WAL is shared
across families; see [RocksDB WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-(WAL)).

The default intended periodic sync interval is 2000 ms, configurable through
`buffered_flush_interval_ms` from 1 to 60000. It is not a disk latency guarantee.
CLI/MCP status advertises accepted/durable sequences, session, pending writes,
batch bytes, oldest age, interval, lag and failure. Reopening first synchronizes
recovered data and starts a fresh session. Older-session and future fences are
refused; inspect/retry the original artifact to determine recovered state.
Fences do not extend retention or certify producer-side drops.

Storage scenarios cover buffered retry identity, count and byte pressure without
prefix publication, foreign/stale/future fence refusal, durable retries and
completed-file import acknowledgments, empty control batches, queue retirement
and reopen. Real-daemon acceptance exercises SDK/CLI/stdio MCP flush, buffered
artifact/event requests, durable mode upgrade, killed-process durable recovery,
old-session refusal and automatic periodic synchronization. These scenarios
qualify the ordinary native API paths; injected host power loss and real failed
disk/WAL synchronization, peak RSS and sustained pressure remain unqualified.

The full `make check` passed with 60 storage scenarios, nine real-daemon/MCP
scenarios, the other workspace checks, release Clippy with warnings denied and
the seeded/pinned helper census. All 68 ledger jobs hold with zero distinct
exceptions or unsanctioned isomorphic groups.

The previous development process was absent and its owned stale socket refused
connections. After verifying no listener/process remained, that endpoint alone
was removed; the database was preserved. The qualified schema-12 executable now
runs under the owned transient user unit `sparrow-development.service`, PID
1857046 at verification, at the existing `development-v5/rpc.sock` and persistent
store identity `store:46c88e949ccec658600b66c7becd5bb7`. This unit survives tool
session closure; it is development supervision, not installed reboot persistence
or qualified crash-restart automation. Inspect it with:

```sh
systemctl --user status sparrow-development.service
target/release/sparrow /home/paudley/.local/state/sparrow/development-v5/rpc.sock status
```

Live buffered event acceptance under `acceptance-schema12-buffered` returned
`artifact:adccc56dea73a970301e12bea670da6f`, 375 bytes, one record, fence sequence
554975 in session `session:273a5f33ffa9525f8e30471fb856779f`. Immediate status
showed one pending write with 161922 batch bytes and a lower durable sequence.
CLI flush reached 554978; an identical request in durable mode returned the same
artifact. Native temporal/full-text retrieval at
`2023-11-14T22:13:20.004Z` recovered
`record:4663ac26601dd8b25ccf6cd886b7217b`, with zero unknown time or failed text
coverage and original source spans. Directory/socket modes remained 0700/0600.
No producer input or sibling/model service was changed.

### Schema 13: artifact, record and pin query membership

Key/value discovery, native text retrieval, combined same-record queries and
context reads now share explicit membership scopes. Generated `Scope.selection`
defaults to live membership, or selects typed evidence references or a stable
pin with an optional revision guard. CLI `--scope-file FILE` and the MCP `scope`
object share one native PurRDF JSON decoder and generated encoder; field and
scope control files share one bounded reader. No alternate parser, tokenizer,
scoring law, or operation envelope was added.

Every selected reference is validated before filters. Complete artifacts expand
their retained records, overlapping references are deduplicated, and missing or
expired selections fail explicitly. A pinned individual record does not make
its expired complete artifact selectable. Explicit field/text scopes seek their
own chronological candidates; context scans apply the same resolved membership
while retaining their existing directional scan budget. Membership expansion
limits and remaining receipt/paging/I/O limitations are documented in
[Interfaces](interfaces.md).

`make check` passed with 62 storage scenarios, ten real-daemon/MCP scenarios,
eight client unit tests, five core unit tests, one server unit test, all census
unit/integration tests and doctests, release Clippy with warnings denied,
seeded census self-tests and the pinned cross-workspace gate. All 72 ledger jobs
hold, with zero distinct exceptions or unsanctioned isomorphic groups. Storage
scenarios exercise artifact expansion/overlap, record seeks, source intersection,
missing references even outside filters, empty selection, pin revision/deadline,
name reuse, and retained records after parent expiry/reclamation. Real-daemon
acceptance verifies all five CLI/MCP query surfaces and a direct generated
foreign-store reference produces a typed `STORE_MISMATCH` result.

On 2026-10-07 at approximately 17:43 UTC, the previous owned development unit
exited successfully on SIGINT, synchronized storage and removed its socket. The
qualified schema-13 executable now runs under `sparrow-development.service`, PID
2250240 at verification, at the unchanged private socket and persistent store
`store:46c88e949ccec658600b66c7becd5bb7`. Store format remains `sparrow.store/5`,
directory/socket modes 0700/0600, and live record count 187. Reopen starts session
`session:b9a51ba13f0bb6147e92fcf41165cf42`. The transient unit still does not claim
reboot persistence or qualified automatic crash recovery.

Live CLI artifact-plus-record selection recovered the existing buffered event
`record:4663ac26601dd8b25ccf6cd886b7217b` from
`artifact:adccc56dea73a970301e12bea670da6f`: key discovery returned 21 key rows,
`/fields/0/value` returned one scalar row, and native text and combined event-time
search each returned one matching record. Receipts reported exactly one eligible
record and no failed projections or decoding failures. The text corpus contained
one document and 57 tokens. MCP independently recovered the same record through
temporary pin `pin:8a8376d2dc852289eb7e85fd264b88d3`, refused revision 2 with
`CONFLICT` and a foreign store with `STORE_MISMATCH`, then durably released only
that acceptance pin. No producer source or sibling/model service was changed.

Full governed SPARQL, diagnostic normalization/correlation, remaining scope
filters and query snapshots/paging, watchers/syslog, retention/pressure/RSS and
corpus performance qualification, and final deployment acceptance remain unmet.

### Completed descriptor upload and verified acknowledgments

The SDK and CLI now route file uploads through `imports::upload`, with one
opened-file generation guard and one native content hash/read path. The public
`Client::import_open_file` and capability-level helper borrow an
already opened completed regular file without reopening its path. Caller
descriptor ownership and cursor survive success or failure; positional reads
start at byte zero independently of shared descriptor offsets. `CompletedFileIdentity`
supplies the same metadata observation/guard to uploader and future claim
coordination. Path imports use safe Rustix `openat2` with symlink resolution
refused and nonblocking special-file opens; tests reject final/intermediate
symlinks, directories and FIFOs. Unsupported kernels fail explicitly.
Rustix 1.1.5 was already locked for dev tooling and is now a direct producer
filesystem dependency; PurRDF's pinned `purrdf-lex` supplies the native JSON
writer. Neither storage nor the SPARQL engine enters the producer graph.

`ExpectedArtifact::certify` is the shared producer receipt owner. Direct inputs,
event batches and completed files must receive their discovered store/session,
source, format, exact byte length/native BLAKE3 identity, admitted membership
and required durability. A stronger durable reply to a buffered request is
valid. Record identities, retention intervals and payload identity syntax are
validated; inspection also refuses a different requested artifact. Metadata
stability is checked after accepted-retry and final publication replies as well
as hashing/uploading, so a late source rewrite produces no certification.

The existing `sparrow.events/1` source codec moved from the daemon adapter to
`EventBatch::source_bytes` in the SDK. Daemon publication and producer payload
verification now invoke one native JSON implementation, preserving its format
and source profile. The bounded delivery worker still retires only certified
durable batches; permanent contradictions preserve the immutable pending body
and charge for diagnosis rather than certify a prefix.

Focused acceptance exercises opened-file upload after source-path replacement,
nonzero descriptor offsets, stable accepted retries and exact daemon byte reads.
The typed real-daemon relay corrupts direct/event/file-retry and final-publication
acknowledgments, substitutes a foreign staging store, and rewrites a source after
accepted-retry/final-publication replies. Every inconsistent receipt is refused;
late rewrites remain on disk, and the original durable commit remains recoverable.
The producer queue reports zero durable events and retains its pending input
on a receipt mismatch. These checks establish uploader/certification behavior,
not completed-consumption deletion authority or claim-journal recovery.

Run the application-owned example with:

```sh
cargo run --release -p sparrow-client --example descriptor -- SOCKET REQUEST_ID SOURCE FORMAT FILE
```

The full `make check` gate passed with 10 client unit tests, 62 storage scenarios,
12 real-daemon/MCP scenarios, the other workspace tests, release Clippy with
warnings denied, seeded census self-tests and all 75 ledger jobs. No distinct
exceptions or unsanctioned isomorphic groups were added.

On 2026-10-07 the owned development daemon was gracefully drained and replaced
with this release build. It retains schema 13, the same private endpoint and
persistent store identity. Live descriptor acceptance published 82 original
JSONL bytes as `artifact:d066e0d527e9e82ec11e13388b386c13`, record
`record:23febe08e38e42deea0f58e4880b9358`, payload
`blake3:b27c4ccab9c865986c3893a6c8916186ab01f3d86e8a72dbedda94dec2f61389`.
Stable retry returned the identical durable receipt; CLI byte export matched
the preserved source. Native key discovery returned four rows and value lookup
returned the exact message scalar. The typed event example also passed native
source-byte certification and stable retry as
`artifact:6ac189d158e9e25014411efc779ac825` (one record, 374 bytes).
The directory remains mode 0700 and its Unix socket mode 0600. Only synthetic
acceptance inputs and the owned Sparrow unit were used; no production source
was claimed or removed and no sibling/model service was managed.

The durable core journal described below now binds source generation and
original publication identity across lost replies and the ordinary retry horizon.
Qualified filesystem claiming/deletion and inotify reconciliation remain required.
No production sample watch is connected; source removal,
full governed SPARQL, other continuous intake, and full architecture acceptance
remain unfinished.

## Durable completed-input ownership

The core now reserves each completed file generation before filesystem claiming.
`consumption_reserve` binds the watch, original relative Unix path bytes and
device/inode/length/modification/change clocks to one native BLAKE3 request
identity. The path is bounded and excludes traversal, NUL, absolute paths and
`.sparrow-inflight` components; non-UTF-8 filenames remain representable. Source
and format cannot change on retry. At most 16 active journals occupy this owner.
The first reservation atomically changes the store marker from `sparrow.store/5`
to `sparrow.store/6` with the journal. The current writer reads both versions;
older writers refuse version 6 instead of reclaiming managed recovery state.
Stores without consumption reservations keep version 5 and their identity.
`consumption_pending` enumerates the bounded active recovery set through the
same journal reader; completed removal tombstones do not occupy active slots.

`consumption_claimed` records the post-rename descriptor observation after the
coordinator has synchronized affected directories. Rename may change ctime, but
device, inode, size and modification clock must agree with the original
observation. Repeated claim confirmation must agree exactly. These core methods
record an admitted coordinator's assertions; they do not inspect paths, prove
quiet-time readiness, rename files or authorize an unlink by themselves.

The reserved `consume:` request namespace uses the existing durable staged
import implementation exclusively. Missing or unconfirmed ownership refuses
admission. Unpublished managed staging survives the ordinary 48-hour import
lease and reclamation; cancellation cannot discard its source recovery state.
The final publication batch atomically records the original artifact in the
consumption journal alongside catalog/record publication and the original
receipt. Receipt reclamation preserves that publication proof while recovery
ownership remains. A missing or contradictory proof is a hard failure, never
permission to allocate another occurrence.

`consumption_removed` requires the original artifact and exact claimed
observation. It records successful coordinator unlink plus directory sync and
archives the journal in ordinary 48-hour completion retry storage, freeing the
active slot. Expired completed requests cannot start another managed import
without a source reservation. Recovery journals are not pins: ordinary 30-day
retention and physical reference retirement still apply, and retry does not
renew acceptance clocks or reconstruct expired evidence.

Storage scenarios exercise reserved/claimed restart, staging beyond its former
lease, atomic publication with a lost reply, recovery after 40 days and evidence
expiry, replacement-generation refusal, idempotent removal confirmation,
completion cleanup, journal capacity and corrupted/missing publication proof.
The anchored filesystem coordinator below now supplies claim/unlink mechanics.
Watch configuration, inotify reconciliation and whole-daemon crash qualification
are still required before completed consumption is exposed as a daemon operation.
Production sample directories remain untouched.

The full `make check` gate passed with 65 storage scenarios, 12 real-daemon/MCP
scenarios, other workspace tests, release Clippy with warnings denied, seeded
census self-tests and all 80 ledger jobs. No distinct exceptions were added.
The matching release daemon was gracefully installed in the owned development
unit on 2026-10-07. It retains schema 13, its original persistent store identity,
mode-0700 directory and mode-0600 Unix socket. With no configured consumption
coordinator it remains `sparrow.store/5`; version-6 admission/reopen is qualified
in temporary storage scenarios. Live descriptor retry recovered its original
durable artifact. An unreserved `consume:` request failed with `CONFLICT`, and
native key discovery confirmed zero eligible records for its source.

## Anchored filesystem claim coordination

`consumption::ClaimRoot` is the sole filesystem coordinator for the persistent
completed-input journal. Its admitted producer root and private claim directory
remain open as directory descriptors. Child resolution uses safe Rustix Linux
`openat2` with beneath-root, no-symlink and no-mount-crossing flags; special-file
opens are nonblocking. Unsupported kernels fail explicitly. The constructor
rejects ordinary path overlap with the actual store, requires a daemon-owned
mode-0700 `.sparrow-inflight` directory, and holds a mode-0600 single-link
regular-file `flock` for exclusive coordination. A bounded private-directory
audit preserves and reports unknown entries instead of silently cleaning them.
Each transfer also checks that the held directory and coordination lock still
have their original linked inode identities; replacing the lock fences the old
coordinator before it reserves or moves another source.
No unsafe code or new semantic codec is introduced.

The journal now includes device/inode bindings for the root and original parent
directory. Its reserved request hash includes those namespace bindings. Current
version-6 journal records require these fields; missing fields fail rather than
rebind a source. No live consumption journal or production watch was created by
this change. A replaced root with active recovery ownership refuses coordinator
startup before creating another claim directory.

`reserve` requires an exact opened-file identity supplied by a readiness/handoff
scheduler. It checks the regular file and synchronizes it and its original
parent before reserving ownership. `recover` uses `renameat2(RENAME_NOREPLACE)`
into the excluded private directory; it checks the named private file against
the opened descriptor before recording a claim. The claimed file and destination
directory become durable before synchronizing source-parent removal and
committing the claim observation. A changed/missing source or occupied target
is refused and preserved for reconciliation. After that checkpoint, the old
parent is no longer an upload/removal capability: its later replacement cannot
redirect the claimed descriptor or prevent completion of the original claim.

`publish` calls the existing SDK `Client::import_open_file` over Cap'n Proto and
verifies the receipt against the same persistent store/journal. It adds no
second uploader, record preparation or publication law. `remove` recovers the
original durable publication proof, checks both held and named generations,
unlinks only the private claim entry, synchronizes its directory and then
records removal. An error leaves source recovery ownership unresolved. Recovery
of a published absent claim synchronizes that absence and completes only its
original publication; an unpublished missing claim never uploads a replacement
from the original path. The configured producer contract still forbids writes
after handoff; identity checks do not lock arbitrary producer file descriptors.

Acceptance shares one in-process storage owner with the production RPC adapter,
generated Unix client and configuration/open-store factory. Synthetic cases
cover coordinator restart after reservation, rename, publication and unlink;
exact original-byte acceptance and stable retry; replacements of old paths and
parents; unpublished removal refusal; late source rewrite; occupied private
names; missing unpublished claims; symlink escape; exclusive ownership;
store overlap; orphan preservation; and root replacement. These are component
and typed-upload checks, not whole-daemon power-loss or inotify qualification.
The daemon's startup configuration reader moved from `main` to `ServerConfig`,
so the fixture does not copy native analyzer or store setup.
Separate admission checks bound FIFO refusal by a two-second test deadline and
verify that a replaced coordination lock leaves the source untouched.

The full gate uncovered an existing race in physical accounting: native RocksDB
compaction can remove a listed file before its metadata observation. The shared
`Writer::physical_bytes` owner now retries the entire measurement at most three
times for disappearing entries. It never returns the sum accumulated before
the failure. Persistent churn refuses accounting/admission with retryable
`STORAGE_PRESSURE`; the producer keeps its input rather than latch a permanent
protocol failure. Missing
root directories, permission errors and other failures remain hard failures.
An owned sidecar create/remove scenario exercises status under directory churn.
This is a bounded point observation, not an atomic filesystem snapshot or whole
store pressure/physical-reclamation qualification.

This coordinator is not yet connected to a production watch. Inotify admission,
recursive bounded reconciliation, quiet timers, configured root overlap rules,
worker backpressure and observable watch administration remain required to
expose completed consumption. No production samples were renamed or deleted.

The full normal commit hook passed with 66 storage scenarios, 14
RPC/filesystem/MCP scenarios, other workspace tests, release Clippy with warnings
denied, seeded census self-tests and all 89 ledger jobs. There are no distinct
exceptions or unsanctioned isomorphic groups. The matching release binaries were
installed in the owned development unit on 2026-10-07 after successful graceful
shutdown. Live status retained the original store identity, schema 13 and
`sparrow.store/5` (no consumption root is configured), with 189 live records and
no pending durability writes. Descriptor retry returned its original durable
artifact and native scalar lookup returned the original message evidence.
The private directory/socket remain mode 0700/0600. This is live regression
acceptance of existing intake/query paths; source claiming/deletion was exercised
only in synthetic component fixtures.

## CLI investigations

Every command starts with `sparrow SOCKET_PATH`. These examples use a synthetic
completed file; production sample locations remain read-only references.

```sh
sparrow SOCKET_PATH ingest case-001 synthetic jsonl /tmp/diagnostic-copy.jsonl
sparrow SOCKET_PATH keys --source synthetic --received-from 2026-10-06T00:00:00Z --received-before 2026-10-07T00:00:00Z
sparrow SOCKET_PATH values --source synthetic --path /message --kind string --decoded 'hello world'
sparrow SOCKET_PATH text 'allocation failed' --source synthetic
sparrow SOCKET_PATH bytes STORE_ID ARTIFACT_ID 0 256 /tmp/evidence.bin
sparrow SOCKET_PATH pin-renew renewal-001 STORE_ID PIN_ID 10 1
sparrow SOCKET_PATH pins STORE_ID --page-size 200
sparrow SOCKET_PATH pin-get STORE_ID --name problem_investigation_1 --page-size 200
sparrow SOCKET_PATH pin-get STORE_ID --id PIN_ID --page-size 200 --cursor CURSOR
```

Use a stable request identity for retrying ingestion of the same source, format
and byte sequence. Conflicting reuse is refused. The importer hashes and uploads
completed regular files through a 64 KiB buffer, resumes the committed offset,
and verifies file stability before publication. It does not delete inputs or tail
active files. Supported explicit formats are
`jsonl`, `log`, `json`, `markdown` and `opaque`; raw preservation is independent
of projection support. Its receipt includes concrete artifact/record IDs and
the daemon's durability acknowledgement.

`bytes` takes store identity, an `artifact:` or `record:` ID, a relative offset,
length (at most 1 MiB) and a new output filename. It writes raw bytes and syncs
the output file; it refuses to overwrite existing files. Its stdout is a JSON
receipt. A local write failure reports that the new output may be incomplete.

`pin-renew` takes an explicit stable request ID, store ID, `pin:` ID, positive
integer days and an optional decimal UInt64 expected revision. MCP's `pin_renew`
uses `request_id`, `store_id`, `pin_id`, integer `days` and optional string
`expected_revision`. Both return the same durable receipt, including direct
members, actual deadline, revision, request identity and durability. Renewal
keeps membership unchanged and uses `max(current_expiry, now + days)`.
Revision mismatches refuse the entire mutation. An identical retry within the
48-hour receipt window returns its original result, including after expiry or
release; that historical receipt does not make the collection active again.
Fresh renewal of an expired/released/unknown ID reports `PIN_NOT_ACTIVE`, and
never follows a replacement that reused its name. Creation/addition and renewal
share one deadline rule, revision increment and synchronized atomic pin/receipt
publication path.
Durations must also remain within the native RFC 3339 clock's representable year
range. UInt64 arithmetic success alone does not authorize an unrenderable
deadline; creation/addition and renewal refuse it before storage publication.

Key/value scope defaults to every retained source and acceptance time, 100 rows
and a 10,000-entry scan budget. `--received-from` and `--received-before` accept
millisecond-exact RFC 3339 instants through the native PurRDF codec. Options
`--limit` and `--scan-budget` select the daemon's bounded work. Value filters
`--path`, `--kind`, `--lexical` and `--decoded` combine as an intersection.
`--decoded` matches exact decoded string equality; it is not token search.
JSON kinds are `string`, `number`, `true`, `false`, `null`, `array` and `object`.
Duplicate/unknown options and invalid intervals are rejected. JSON output keeps
unsigned counts as decimal strings and includes the daemon's completeness and
coverage receipt. Transport/domain failures exit unsuccessfully without a
successful result prefix.

Development configuration selects
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock` and the database in its
parent directory. The directory is mode 0700 and socket mode 0600. The daemon
refuses an existing endpoint rather than unlinking a listener's socket. An owned
dead endpoint must be reconciled before restart after an unclean shutdown.
Connections have a 60-second lease. A configured endpoint is not evidence of a
running server; verify startup and a typed `status` call.

Ingestion acknowledges only after the atomic RocksDB write is synchronized. A
connection failure before acknowledgement leaves delivery unknown; retry with
the same request identity. Ingestion and pin/create/renew/release receipts last 48 hours.
Expired evidence metadata has a one-day tombstone window, except live pinned
records. Retention maintenance runs every 30 seconds.

## Storage and queries

Core storage implements exact-byte content-addressed chunks, independent
occurrences, references, thirty-day acceptance retention, named overlapping
pins, record survival after parent expiry, bounded uploads and durable release
retries. PurRDF runtime dependencies are locked to
`e58974e504b9681597faea1490002ce8ddaf7448`.

JSON search facts come from `purrdf::json::analyze`. They preserve member order,
duplicate names, exact RFC 6901 pointers, native kinds, number spellings and
original spans. Full content digest and native profile identity bind shared
facts. Decoded equality uses PurRDF's lexical decoder. Parser failures keep raw
evidence and appear in query coverage.

The native JSON fact profile caps a record at 16 MiB source bytes, 65,536 values,
depth 128 and 2 MiB aggregate pointer text. Atomic ingestion batches are capped
at 128 MiB. `bytesRead` is capped at 1 MiB. Query limits are 1..1000 rows and
1..100000 catalog/value visits. Scans and response construction are bounded;
limited work is explicitly reported.

`keysList` and `valuesSearch` use temporal catalog scans over stored native
facts. These are not yet field-posting indexes. Scope selects exact source and
a half-open server-acceptance interval in typed Unix milliseconds. Event-time
interpretation remains pending. Value predicates include existence, kind,
exact lexical equality, decoded string equality and an exact pointer; no
wildcard selector is inferred.

## Stdio MCP

Launch the adapter with the existing private daemon socket:

```sh
target/release/sparrow /home/paudley/.local/state/sparrow/development-v5/rpc.sock mcp --stdio
```

A host configuration can use this command and argument array:

```json
{
  "mcpServers": {
    "sparrow": {
      "command": "/home/paudley/Active/sparrow/target/release/sparrow",
      "args": ["/home/paudley/.local/state/sparrow/development-v5/rpc.sock", "mcp", "--stdio"]
    }
  }
}
```

The adapter implements MCP protocol 2025-11-25 initialization, initialized
notification, ping, tools/list, tools/call and cancellation over newline-delimited
stdio. It uses PurRDF's JSON codec and native canonical base64 formatter. The
daemon transport remains typed Cap'n Proto; the adapter never opens RocksDB.
Only protocol messages go to stdout. Each tool opens its own owned RPC connection,
so an idle MCP session does not retain an expired daemon connection lease.

Discovery advertises these implemented tools only: `capabilities`, `status`,
`keys_list`, `values_search`, `text_search`, `records_context`, `artifact_get`, `record_get`,
`bytes_read`, `rdf_inspect`, `pins_list`, `pin_get`, `pin`, `pin_renew` and `pin_release`. Input schemas reject additional fields;
query arguments use the CLI's names with underscores. Temporal filters remain
explicitly server-acceptance times. Offsets are decimal strings; bounded lengths,
row limits and durations are JSON integers. Byte results use native canonical
base64 by default or explicitly requested UTF-8, which refuses invalid bytes.
Pin/release calls require stable mutation request identities. Read/mutation
annotations accompany tool discovery.

Responses include structured content and the same JSON in a text block. Daemon
domain errors preserve code, message and retryability in structured tool errors;
transport failures have the distinct `RPC_FAILURE` code. Unsupported tools and
malformed protocol requests produce JSON-RPC errors. Undelivered or cancelled
mutation acknowledgement does not prove non-delivery: retry its stable identity.

The adapter bounds requests to 128 KiB, responses to 8 MiB, concurrent calls to
four and each call to 30 seconds. Oversized framing closes the session with an
explicit protocol error; malformed complete JSON frames receive a parse error
and subsequent valid frames can proceed. EOF drains pending calls. Discovery
requires daemon schema version 8, including finite pin catalog captures, durable pin renewal receipts, RDF profile identity, record context
and native text/profile identities;
older discovery contracts fail explicitly rather than invent those identities.

The real-daemon acceptance fixture checks CLI/MCP parity for keys, text and context, JSON
value matches, exact base64 bytes, store identity errors, pin/create/renew/release retry and
capabilities before and after daemon crash/reopen. An isolated unresponsive
socket fixture checks pending-call admission, failure during a partial input
frame, cancellation and oversized framing. Integer request IDs beyond binary64
exact precision round-trip without rounding. The full release `make check`
gate passed with these fixtures.

## Pin catalog pages

The typed `pinRead` selector union supplies active collection listing, stable-ID
lookup and case-sensitive name lookup. CLI `pins STORE_ID` and `pin-get STORE_ID
(--id PIN_ID | --name NAME)` share MCP `pins_list` and `pin_get` operations and
one renderer. MCP requires `store_id`; `pin_get` requires exactly one `pin_id` or
`name`. Both accept optional `cursor`, `page_size` and `scan_budget`. CLI spells
those flags with hyphens. Defaults are 200 requested items and 10,000 catalog
entry admissions; allowed ranges are 1..1000 and 1..100000 respectively.

Fresh reads capture eligible pin metadata at one server clock instant. Collection
rows and member references use stable ID order. Name lookup resolves once to a
stable ID, revision, actual deadline and direct members. Captured pages preserve
that result after add/renew/release, expiry or reuse of the name. The receipt
states `snapshot_id`, `as_of`, `lease_expires_at`, complete/exact execution,
catalog coverage, total items, page position, effective limits and continuation.
Pagination does not downgrade a complete capture into incomplete execution.
A cursor requires its original operation, selector and limits; changed scope is
`CONFLICT`. A retry of the same cursor returns the same domain page through CLI
and MCP. Clients must replay opaque cursors without interpreting their encoding.

These captures retain metadata only. They do not renew pins, preserve original
evidence, keep a RocksDB snapshot open or hold the catalog writer between pages.
A listed member can become unavailable to a subsequent evidence read. The absolute
lease is 60 seconds under both server UTC and a monotonic clock. Pages never extend
it. Expired/unknown/restarted-owner handles refuse with `RESULT_EXPIRED`; fresh
lookup of inactive IDs/names refuses with `PIN_NOT_ACTIVE`. New reads and periodic
maintenance reclaim expired handles. Status reports handle count, admitted
retained capacity, the eight-handle ceiling, 8 MiB capacity ceiling and lease.

One capture admits at most 10,000 items and 2 MiB retained capacity. Catalog entry
decoding is limited to 2 MiB, depth eight and 10,032 native JSON values; one capture
inspects at most 32 MiB of original catalog bytes. Scans, native storage failures,
invalid catalog identities, duplicate membership or conflicting active names
refuse the entire capture without publishing a successful prefix. Capacity growth
is admitted before extending the captured vector. Pages conservatively reserve
2 KiB per item plus 4 KiB envelope space under a 256 KiB structured ceiling, so
the requested item count is an upper bound. These are host admission capacities,
not process RSS or physical RocksDB I/O measurements. Broader query pagination
and concurrent stored-RDF snapshots remain required work.

Blocking storage workers own their concurrency permit until the worker actually
finishes. Cancelling or disconnecting the awaiting RPC does not free admission
early and create an unbounded blocking-work queue. A regression test pauses a
worker, cancels its RPC task, verifies admission remains occupied, then releases
the worker and verifies the permit returns.

## Retained record context

```bash
target/release/sparrow "$SOCKET" context "$STORE_ID" "$RECORD_ID" \
  --before 10 --after 10 --ordering received_at_record_id
```

The `records_context` MCP tool takes `store_id`, `id`, optional `before`/`after`
counts (default 10 each), `ordering` (only `received_at_record_id`), and the
same source/acceptance-interval/limit/scan-budget arguments as searches. Counts
may be zero. The total including the anchor must fit `limit` (default 100,
maximum 1,000); invalid combinations are refused rather than silently reduced.
An explicitly supplied source or interval must include the anchor. Expired or
evicted anchors fail with `EVIDENCE_UNAVAILABLE`; reclaimed metadata fails with
`NOT_FOUND`. References from another store fail with `STORE_MISMATCH`.

The operation captures membership under the serialized storage owner and scans
the persisted time index outward from the anchor. It returns same-source retained
record summaries, their exact artifact-relative byte bounds and a common receipt.
Both neighbor lists are ascending by `(received_at, record_id)`. This is acceptance
order, not file byte order or inferred event time: all records in a completed
artifact share acceptance time, so their opaque record IDs break ties.

Each direction independently applies `scan_budget` (default 10,000, maximum
100,000 catalog entries). Its result includes `visited`, `retired` and a frontier:
`requested_count`, `scope_boundary` or `scan_budget`. Observed expired/evicted
entries from that source count as retirement gaps; entries from other sources
consume scan visits without appearing as neighbors. Budget exhaustion gives
limited/incomplete execution rather than an assertion that no neighbors exist.
Reaching the requested count is a complete bounded context request.

`continuity_unknown=true` is explicit: tombstones already reclaimed and producer
capture gaps cannot be inferred from the current catalog. Zero observed retired
entries does not prove continuous producer history. Context uses record metadata,
not JSON/text indexes; their projection coverage is not assessed by this operation.
It creates no pin and does not extend retention. A pinned record remains a valid
anchor after its unpinned complete parent artifact expires. Use `bytes_read` and
explicit pins to inspect and preserve the returned evidence.

Core tests compare neighbors against the stored timestamp/ID order and cover
other sources, interval boundaries, zero counts, budgets, reopen, expired anchors,
record pins, observed gaps and corrupted time-index refusal. The real-daemon
fixture checks shared CLI/MCP context results and typed ordering/store errors
before and after crash/reopen.
File/stream order, event-time context, pagination leases and producer continuity
qualification remain required later work.

## Persistent RDF foundation

Store format 4 bound the RDF layout, native JSON profile, native Markdown contract,
and admission limits to a persisted `rdf_profile` identity. Missing identity is a
storage failure; drift refuses reopening with a migration-required error. Earlier
format-3 data is preserved separately; this implementation does not migrate it.

Native JSON analysis supplies both diagnostic field facts and the full native RDF
projection. Markdown uses native slicing and parses its emitted Turtle through
PurRDF. Terms persist as native canonical `ProjectionTerm` JSON with a verified
forward/reverse dictionary and monotonic nonzero 64-bit IDs. Nested triple terms,
blank scopes, literal language/direction and datatype identity survive storage.
Primary, reifier and annotation streams remain separate, each with physical SPOG,
POSG, OSPG, GSPO, GPOS and GOSP indexes. Declared empty named graphs persist.
Native default graph rows are lifted to the content owner's named graph; explicit
native graph names remain intact. Owner IDs distinguish shared graph membership.

Identical content shares its projection graph while each occurrence has its own
record metadata graph. Metadata describes source, artifact, original byte range,
payload identity, acceptance/expiry and projection state. Pin-aware expiry removes
all six indexes and dictionary reference closures only after their final graph
owner expires. Raw evidence survives projection refusal with a diagnostic.

Graph admission caps rows at 32,768, term encoding at 64 KiB and nested depth at
16, dictionary staging at 32 MiB and anticipated RDF batch staging at 64 MiB.
These are admission bounds, not a measured process RSS guarantee. Metadata and
content publication share the raw-evidence atomic write batch.

`sparrow SOCKET_PATH rdf STORE_ID RECORD_ID` and MCP `rdf_inspect` expose retained
record metadata/content graph identities, projection state, diagnostic and exact
counts for each native stream. This inspection does not run SPARQL or reconstruct
a resident dataset. Native stored-view and governor qualification remain required
before semantic graph queries can be advertised.

Qualification compares every persisted JSON quad against native projection in
all six indexes, checks shared content and pinned expiry, exercises Markdown and
oversized/malformed projection refusal with exact-byte preservation, and verifies
RDF 1.2 nested terms, distinct blank scopes, directional literals, reifier and
annotation layers, and declaration-only empty graphs. Profile drift/missing
identity and shared CLI/MCP inspection are covered by the repository gate.

## Artifact discovery and shared catalog leases

`Store::artifact_read` captures eligible complete artifacts and the parent
provenance of surviving records. Identical payloads retain separate artifact
occurrences. Summaries distinguish complete byte availability, exact eligible
record count and optional original metadata. After parent catalog reclamation,
the source and parent ID remain discoverable through surviving records, but the
original format, receipt time, byte count and original record count are unknown.
Fragment byte counts and record timestamps never substitute for those values.

The initial selector supports an exact source label and a half-open original
artifact receipt interval. A time-filtered capture with an applicable reclaimed
parent refuses with `INDEX_NOT_READY`; a known source mismatch can exclude it
before that check. Whole-artifact pins preserve complete empty inputs as well
as nonempty ones. Record pins preserve surviving parent provenance without
claiming downloadable complete files. The capture uses the existing retention,
protection, event-record validation and native catalog decoding owners.

One `catalog_lease` implementation now serves both pin and artifact captures.
It owns absolute wall/monotonic expiry, page construction, cursor validation,
operation/selector/limit binding and publication admission. The original pin
reader routes through it; its specialized name selects the generic page type,
without retaining a second paging implementation. Capture limits are shared
constants. Each reader has at most eight retained handles and 8 MiB of admitted
metadata, with a 2 MiB per-capture ceiling. The artifact capture inspects at most
32 MiB of catalog bytes, refuses entries over 512 KiB before native decoding,
and admits at most 10,000 summary rows. Each page has a conservative 256 KiB
structured ceiling, which can reduce the requested item count. All pin/artifact/
record visits consume the request's scan budget. Exhaustion or corrupt evidence
publishes no successful prefix or handle.

Pages preserve their captured availability/counts after ingestion or retirement;
`as_of` identifies that observation. A subsequent byte request validates current
availability independently. Captures neither pin bytes nor retain database
versions. Expiry maintenance and status purge expired handles even when clients
stop reading. Core status exposes artifact handle/byte counts and limits.

Schema 24 now connects this reader to typed `artifactsList`, CLI `artifacts
STORE_ID` and MCP `artifacts_list`. CLI and MCP share one native client and
renderer. Pin/artifact receipt encoding and rendering also each have one owner.
Typed status includes artifact handle counts, capacity ceilings and lease
duration. Full live/pin/selection and diagnostic scope support, source discovery
and corpus-scale indexed selection remain unfinished requirements.

Local qualification on 2026-10-07: final `make check` passed formatting,
release Clippy with warnings denied, all workspace release tests (100 storage
and 28 RPC/daemon/MCP scenarios), seeded census self-tests and the pinned
cross-workspace gate. All 147 ownership jobs hold with no distinct exceptions.
Evidence is `/tmp/sparrow-artifact-catalog-final-check-20261007.log`.
Artifact scenarios verify immutable source/time pages after later ingestion,
distinct repeated occurrences, changed-selector/limits and foreign-store cursor
refusal, lease expiry, surviving record parents with unknown origin metadata,
whole-artifact empty pins, frozen observations after release/retirement, no
published prefix on scan/corruption failure, full admitted 512-byte source labels
and the shared-by-clones handle ceiling. Existing pin paging and monotonic expiry
scenarios pass through the shared lease owner. No runtime dependency, store format,
wire schema or PurRDF pin changed. This does not qualify the unfinished typed
artifact-list endpoint or larger-corpus/indexed selection requirements.

## Bounded stored RDF reads

### Concurrent native snapshots

Stored RDF reads now capture one native RocksDB snapshot while validating
selected record membership, projection readiness and active pin protection under
the writer owner. Execution releases that owner. Point reads, forward/reverse
term validation, quad probes and graph declarations all use the same snapshot.
The native sequence number is exposed by `snapshot_sequence_number`; it is
read evidence rather than an occurrence identity or an investigation pin.

The database handle is shared only for lifetime ownership. Snapshot scope does
not retain source records in the current catalog or renew their expiry. A query
may finish reading its captured versions while concurrent pin release and
retention remove current evidence; later reads revalidate current eligibility.
Subsequent ingests are excluded from the captured dictionary and graph state.
The existing bounded lease, sticky resource failure and native checkpoint
boundary still apply. Native snapshot versions can delay physical reclamation;
the pressure worker already observes the native snapshot count. Snapshot release
occurs when the callback exits, including failed execution. A callback that never
returns is not preempted by the storage lease.

One native RDF snapshot per store is admitted across all store clones; another
request returns retryable `RESOURCE_LIMIT` rather than retaining another native
version or waiting in an unbounded queue. This preserves the former single-query
memory admission while allowing ingestion and maintenance to run during it.
The admission slot is released on callback return or a typed error.

The concurrency scenario releases the selected record's protection, retires it
and ingests a new record from another thread while the callback is active. It
requires that writer to finish before the callback continues, verifies unchanged
quads/graph declarations and old-term resolution, refuses lookup of the later
graph, then checks that a new read sees the later sequence and rejects the
retired record. This is native stored-view qualification, not completion of the
CLI/MCP query snapshot, result-handle or pagination contracts. Full SPARQL still
requires the upstream native workspace admission described below.

Local qualification on 2026-10-07: the final `make check` passed formatting,
release Clippy with warnings denied, all workspace release tests (96 storage
and 28 RPC/daemon/MCP scenarios), seeded census self-tests and the pinned
cross-workspace gate. All 145 ownership jobs hold with no distinct exceptions.
Evidence is `/tmp/sparrow-rdf-snapshot-final-check-20261007.log`. The final
concurrency test also verifies shared admission refusal and successful admission
after the first snapshot is released. Existing native semantic, storage-failure
and governor scenarios remain passing. No wire schema, store format, runtime
dependency or PurRDF pin changed. The owned development daemon remained active;
this local stored-view qualification does not claim a live SPARQL RPC endpoint.

Store format 5 adds atomic dictionary cardinality, a conservative encoded-term
high-water bound, and graph-declaration postings. Opening a native view does not
scan the dictionary or decode each graph manifest. Missing or inconsistent control
counters are hard storage failures. Format-4 data remains preserved separately
and requires explicit migration before reopening with this implementation.

`Store::with_rdf_records` admits one concrete selection of 1..1000 retained
record IDs. It includes occurrence metadata and complete shared content, validates
the selection atomically, and refuses failed applicable projections. Pin-aware
eligibility uses the same retention law as ordinary reads. Scope preparation,
pin parsing, term access, lazy index scans and native operator scratch share one
read budget; ordinary-age selections do not scan pins. Scope capture holds the
serialized store owner; execution reads its immutable snapshot after releasing
the owner. The full live/pin/selection query scope interface remains required work.

The default ceilings are 128 MiB live admitted capacity, 100,000 read admissions,
128 MiB admitted maximum read bytes, 100,000 index-row admissions, and a 30-second
lease. A lease may be configured up to 60 seconds. These are host admission
counters, not measurements of engine I/O or process RSS. Native engine caches and
whole-process qualification remain separate. Terms use their persisted encoded
high-water bound times 256 plus 4 KiB as conservative decoded/owned capacity;
lookup inputs are admitted before native conversion can clone them.

Native `DatasetView` and `FallibleDatasetView` read all three physical streams,
preserve declared empty graphs, and use physical prefix probes. Each stream also
exposes its virtual default set union while retaining named graph identities.
Adjacent owner/graph duplicates are suppressed without a resident row set. Typed
guards retain portable native term carriers and resolve datatype/triple links
through the same verified forward/reverse dictionary. Compact IDs remain scoped
to this read; they are not agent evidence references.

All read/lease/admission failures latch the first root cause and stop lazy
iterators. Native before/after checkpoints discard internal prefixes. Tests cover
cross-record native SELECT joins, selected-owner isolation, all bound-axis shapes,
native RDF 1.2 terms/layers, empty graphs, pin survival, exhaustion, missing terms,
malformed index/declaration identities and dictionary reclamation. The required
full SPARQL algebra remains unmet; see
[PurRDF integration gaps](purrdf-integration-gaps.md). No SPARQL tool is advertised.

## Persistent full text

Store format 3 binds native standard analyzer identity at atomic store creation.
The configured `text_lexicon_dir` must contain exactly five standard CBOR
lexicons; native `Analyzer::resolve` verifies their pinned physical identities.
Missing or changed artifacts refuse startup. A changed analyzer identity refuses
reopening without explicit migration. Format 2 stores are preserved and require
migration. The subsequent RDF layout uses format 4 and a separate
`development-v4` directory; format 3 stores also require explicit migration.

Ingestion stores native lexical terms, record token lengths, sparse frequency
postings and separate source-evidence postings in the same synchronized batch as
raw evidence. Expiry removes those postings with the owning record; pins retain
them. Queries never reread or reanalyze retained payloads. The initial record
search profile indexes raw UTF-8 text and decoded JSON string scalars together
as one explicitly named field. Non-UTF-8 and oversized records retain raw bytes
and report failed text coverage; opaque inputs report not applicable.

`text QUERY` uses the same source/time and work-limit options as `keys`. Native
query analysis and prepared BM25 scoring rank records matching any analyzed
query term. A digest identifies captured document membership, token lengths,
store and analysis/ranking laws. Query output includes profile identities,
population and token totals. Do not compare scores across different corpora.
When membership scanning is limited, ranking is over the explicitly incomplete
captured membership. Posting-budget exhaustion refuses ranked publication,
rather than scoring with incomplete document frequencies. This is bounded
stored-fact retrieval, not yet the governed SPARQL text relation or a pagination
lease.

The aligned text profile currently bounds a record at 1 MiB and 16,384 lexical
occurrences across raw and decoded views. Evidence output is at most 64 spans
per returned record, with an explicit `evidence_limited` flag. Raw matches retain
native contributors and highlights; decoded-string matches conservatively cite
the complete original JSON scalar with `coarse=true`. Membership and posting
scans each use the requested scan budget. Document lengths are persisted;
corpus/term populations are measured from the captured scope. Source/time corpus
aggregates and further posting access paths remain performance work.

## Verification and remaining work

The release-profile `make check` gate passed on 2026-10-06, including formatting,
Clippy with warnings denied, workspace tests and the helper-census self-test and
cross check. The real-daemon Cap'n Proto test passed for ingestion, original byte
retrieval, temporal key discovery, decoded string search, malformed JSON
coverage, typed store mismatch, durable occurrence identity after crash/reopen,
durable pin-release retry and rejection of partially valid pin requests. All
eighteen core storage/clock/query/text/context tests passed in the release gate.

The same daemon fixture also exercises the CLI before and after crash/reopen:
stable ingest identity across RPC and CLI, unchanged input bytes, oversized
import refusal, exact byte export and overwrite refusal, time-scoped duplicate
keys, decoded value matches, source exclusion, explicit limited execution and
invalid command rejection.

The latest release workspace build and full `make check` passed. The format-3
development daemon was started and its typed status client succeeded on
2026-10-06 local time (2026-10-07 UTC) with identity
`store:4b5ebc0de33ba66b6bf0892db5168bca`. A synthetic two-line log was durably
imported under `diagnostic-demo`; `text failed --source diagnostic-demo` returned
one record, native BM25 score `0.628834555557`, complete/exact coverage, and the
original `failed` span `[11,17)`. The query returned analyzer and corpus digests.
Directory mode 0700 and socket mode 0600 were verified. The owned format-2
development process was cleanly stopped after confirming its identity and empty
store; its database directory was preserved. These are development runtime
observations, not final architecture acceptance or a guarantee of later liveness.

The daemon was subsequently replaced with the qualified schema-version-2 build
on 2026-10-07 UTC, reopening the same format-3 store and preserving its identity.
Its typed status reported the persisted analyzer identity and text profile. A
live stdio MCP session initialized, listed all ten implemented tools, retrieved
capabilities and repeated the synthetic `failed` search. It returned the same
record, corpus digest and original span `[11,17)`, then exited successfully on
EOF. The private directory/socket modes remained 0700/0600.

The qualified context build then reopened that store with discovery schema
version 3 on 2026-10-07 UTC. Live CLI and stdio MCP context calls returned the
same synthetic anchor and one retained same-source neighbor, with explicit
`received_at_record_id` ordering and unknown producer continuity. MCP listed
eleven implemented tools. The store identity and analyzer/corpus evidence were
preserved, and directory/socket modes remained 0700/0600. No producer data was
consumed or deleted during these runtime checks.

The qualified RDF build started a fresh format-4 store on 2026-10-07 UTC at
`development-v4/rpc.sock`, store ID
`store:c0c19ff0de784da626470803a3274dbc`. The owned format-3 daemon stopped cleanly
and its database remains intact. Live JSONL record
`record:cc036f2592587ba42e4d947bf2c50284` has 80 native primary content rows and
11 metadata rows; Markdown record `record:5dd10b50225185eb43f38181eca8a579` has
41 content rows and 11 metadata rows. CLI and MCP inspection agreed; MCP advertised
twelve tools, drained EOF successfully and produced no diagnostic stdout.
The persisted RDF profile identity is
`1ceeeb7b44784d4db19d7696a8978e50c08961ddebceecd9934f61bd2f9f056f`.

The synthetic two-line log retained all 75 bytes exactly, returned the native
`failed` span [11,17), and exposed its same-source neighbor through context.
The 70,015-byte oversized-term JSON fixture produced an explicit failed RDF
state with no content graph; its original bytes were exported and compared exactly.
Directory mode 0700 and socket mode 0600 were verified. Only synthetic fixtures
were ingested; no production watches or sibling services were changed.
The full repository gate passed, including refusal rollback, native RDF 1.2
identity/layers, all six index permutations, pin-aware reclamation, profile drift,
and daemon/MCP crash-reopen coverage. Stored-view SPARQL remains unimplemented.

The bounded-reader build started a fresh format-5 store on 2026-10-07 UTC at
`development-v5/rpc.sock`, store ID
`store:46c88e949ccec658600b66c7becd5bb7`, RDF profile
`1909d176e2b728f3041d0b2c665600aed5303e4b8525260bc1165850690ad0d5`.
The owned format-4 daemon stopped cleanly and its database remains intact.
Live checks imported five synthetic occurrences. Two identical JSONL records
shared one 80-row content graph while retaining separate 11-row metadata graphs.
The Markdown record retained 41 content rows. All three fixture artifacts exported
their exact original bytes. CLI/MCP RDF inspection agreed, MCP listed twelve tools
and drained EOF without extraneous output, and text/key/context checks passed.
Private directory/socket modes remained 0700/0600. No production inputs were
consumed and no sibling services changed.

The required release gate passed 24 storage scenarios, four core unit tests,
daemon/MCP crash-reopen checks and eleven enforced helper jobs with no duplicate
exceptions. The Rust core's native SELECT join is qualified on persisted fixtures;
the daemon does not expose SPARQL. Native workspace admission for the required
remaining algebra is tracked in [PurRDF integration gaps](purrdf-integration-gaps.md).

Remaining final-server acceptance includes concurrent stored-view qualification,
governed SPARQL, source/artifact metadata graphs, further
text qualification, event-time and combined predicates, context and
correlation, final scope/pagination contracts, producer import recovery
journals, durable completed-file consumption, inotify/tailing, remaining MCP/CLI tools,
syslog, the Rust logging client, pressure eviction/reclamation, codec
verification, bounded buffered durability, fault/performance qualification,
packaging and a verified running-server handoff.

The pin-renewal build reopened the same format-5 store on 2026-10-07 UTC with
discovery schema version 5. The previous owned daemon stopped cleanly; the five
synthetic records, store identity and RDF/text profiles remained unchanged.
Live MCP discovery listed thirteen tools and marked renewal as a mutation.
Fixture pin `pin:6487733f54effd194871768af71c3247` was created over
`record:6b962236ac743284a0598093966dd49a`, renewed with an expected revision,
and released. CLI and MCP returned identical revision-2 durable receipts with
deadline `2026-10-09T06:35:39.756Z`. Retrying renewal after release returned that
same historical receipt while status still reported zero active pins. The MCP
session drained EOF successfully without extraneous stderr. Only this synthetic
pin and the owned development daemon were changed.

The finalized renewal release gate passed 27 storage scenarios, four core unit
tests, the real-daemon/MCP crash-reopen and protocol tests, and twelve helper
jobs with no duplicate exceptions. Renewal coverage includes unchanged members,
non-shortening deadlines, simultaneous revision guards, changed-argument retry
conflicts, post-release/expiry retries, name reuse, the exact expiry/retry
boundaries, reduced membership limits after reopen and native RFC 3339 deadline
refusal before commit. Full server acceptance remains unfinished.

The five standard text lexicon artifacts are available in the sibling PurRDF
checkout under `crates/text/lexicons/artifacts/`. Native `Analyzer::resolve` must
verify their physical identities and bind the fingerprint to persistent text
projections. Do not substitute an empty lexicon or rebuild the retained corpus
as a resident index on each query.

The pin-catalog build reopened the same development format-5 store on
2026-10-07 UTC with discovery schema version 6. The previous owned daemon stopped
cleanly; store identity, five retained records and RDF/text profiles remained
unchanged. Live MCP discovery listed fifteen tools and marked `pin_get` and
`pins_list` as reads. Fixture pin `pin:829f88b26f505dfb273587d5551e3a81` captured
two direct members under snapshot `pin-read:c244c8654217ed0f6daed1506d4cb5a7`,
as of `2026-10-07T07:03:20.367Z`, with absolute lease deadline
`2026-10-07T07:04:20.367Z`. It was renewed, released and replaced by
`pin:f08d7a4d708309955220018177807f00`. The original cursor still returned its
revision-1 metadata and original deadline while fresh name lookup resolved the
replacement. The collection snapshot likewise retained its original two IDs.
CLI/MCP continuation receipts were identical for both operations. All fixture
pins were released; immediate status reported zero active pins, five records,
two finite handles and 2,714 admitted bytes. EOF drained with no extra stderr.
Private directory/socket modes remained 0700/0600. No production inputs or
sibling services were changed.

The catalog-page gate passed 31 storage scenarios, five core unit tests, the
server worker-cancellation regression, real-daemon/MCP crash-reopen tests and
thirteen enforced helper jobs with no duplicate exceptions. Tests distinguish
pagination from incomplete capture, exercise member/collection stability after
mutations, exact UTC and monotonic expiry, capacity/scan refusal, smaller pages
under the output ceiling, catalog identity faults, source-byte retirement while
metadata pages survive, and cursor invalidation on restart. Full first-version
server acceptance remains unfinished.

## Combined record qualification

`recordsSearch` (discovery schema version 7), MCP `records_search`, and CLI
`records` share one storage operation. Optional native text ranking runs over
records that satisfy every field constraint, rather than joining matches from
separate records in the same artifact. Without text, rows are chronological
acceptance-time summaries and have no fabricated score or ranking corpus.
The existing `textSearch` uses this same operation with no field constraints.

For example, a client-local JSON file containing
`[{"path":"/job","decoded":"fixture-7"},{"path":"/level","decoded":"ERROR"}]`
can be passed to:

```sh
target/release/sparrow SOCKET records --source diagnostic-jsonl \
  --text 'allocation failed' --where-file predicates.json \
  --received-from 2026-10-07T00:00:00Z --received-before 2026-10-08T00:00:00Z
```

MCP accepts the same array as `fields`, with optional `text`, `source`,
`received_from`, `received_before`, `limit`, `scan_budget`, and
`require_complete_index`. The criteria file is read only by the CLI; its path is
never sent as a server read capability. Unknown fields are refused.

Each constraint intersects native `path`, `kind`, `lexical`, and `decoded`
conditions on one occurrence. Lexical equality uses the retained native scalar
span, including JSON escape spelling; native string spans cover the contents
inside the quotes. All constraints qualify the same record. Constraints sharing
a `parent_binding` label must have the same native parent occurrence identity.
An optional `parent_path` chooses an exact ancestor pointer; absent it, the
immediate parent is used. Equal pointers in duplicate members do not make two
parents identical. Root binding uses `parent_path:""`. These are exact concrete
pointers; no additional JSONPath interpreter is introduced.

Rows include one supporting occurrence per predicate, its concrete path/kind,
original artifact byte bounds, and any bound parent identity. All metadata and
witnesses are captured while the storage owner is locked. Text rows, native
scores, ranking corpus identity and evidence are returned separately in the
optional typed `text` result. Raw payloads are not reread or reanalyzed.

A query admits at most 32 constraints and 256 KiB total field criteria, with
64 KiB optional text. Field occurrence/predicate/ancestor work has its own
`scan_budget` ceiling and refuses exhaustion without a result prefix. Membership
and posting work retain their reported existing limits. Chronological rows have
a 2 MiB response ceiling; ranked membership has a conservative 64 MiB capture
ceiling, measured as encoded metadata/witness bytes plus per-record allowance,
not a process RSS guarantee. Applicable failed JSON/text projections are counted;
`require_complete_index:true` refuses them. Non-JSON records are counted as
not applicable when JSON fields are requested. Limits and incomplete coverage
remain visible in the receipt.

The schema-7 operation selected acceptance time only. The explicit source-time
extension below adds event intervals in schema 8. Cursor continuation,
pin/selection query scopes, higher-precision/pre-epoch clock representations,
and the full native governed SPARQL interface remain required work.

Combined-query qualification passed `make check`: 33 storage scenarios, five
core unit tests, the server admission/cancellation unit test, 50 census tooling
unit tests, both real-daemon/MCP scenarios (including crash/reopen), seeded census
self-tests, and all 14 pinned helper-ledger jobs with no distinct exceptions or
unsanctioned isomorphic groups. Real-daemon tests cover CLI/MCP row parity,
field-only absence of text results, root occurrence `0` as a present binding,
strict failed-index refusal, and refusal of unsupported event-time arguments.

The owned development daemon was updated on 2026-10-07 at approximately
07:29 UTC to discovery schema 7, PID **446585**, using the same configuration,
private socket, and `store:46c88e949ccec658600b66c7becd5bb7`. Its prior process
exited cleanly on SIGINT. Status retained five records, four payload chunks, one
JSON projection and no active pins or pin read handles; store format remains
`sparrow.store/5`. No migration, production import or producer lifecycle action
was performed.

Live CLI and stdio MCP combined queries for `diagnostic-jsonl`, text
`allocation failed`, `/job` decoded `fixture-7` and `/level` decoded `ERROR`
returned the two existing synthetic JSONL occurrences with complete/exact
receipts. Both predicates bind root occurrence `0`. Original `/job` evidence is
`[84,93)` and `/level` evidence `[40,45)`. Field-only queries return the same
metadata/witness rows with `text:null`. Native ranking uses two documents and
48 tokens, corpus
`8e28d99ceee6e7073cbef16582bbe985fa97bb420ad62785f5015f5003c2675c`, identical
through CLI and MCP. The MCP adapter exited cleanly on EOF.

## Explicit source-time profiles and event intervals

Discovery schema 8 adds typed `sourceTimeDefine` and `sourceTimeGet` RPCs,
MCP `source_time_define` and `source_time_get`, and CLI
`source-time SOURCE POINTER REPRESENTATION` / `source-time-get SOURCE`.
A definition durably binds one exact source name to a concrete native JSON
pointer and one declared representation. Identical retries return the same
content-addressed profile; conflicting definitions return `CONFLICT` and require
a new logical source name. Define the profile before importing that source.
A later definition never reinterprets older records or changes an ingestion
retry receipt. Profile lookups return explicit absence for undefined sources.
The registry admits at most 1,024 mappings, bounds source names at 512 bytes and
pointers at 4,096 bytes, validates native pointer escapes and catalog identity,
and refuses physical control-admission pressure.

Representations are `rfc3339`, `unix_millis`, and `unix_seconds`. RFC 3339 values
must be native decoded strings with an explicit offset, at or after the Unix
epoch, exact to the existing millisecond clock. Integer epoch representations use
PurRDF's native XSD integer lexical reader and checked unit conversion. Fractional
or exponent-form numeric values are not silently rounded or guessed as another
unit. Out-of-range, pre-epoch, ambiguous-zone and unsupported precision values
remain unknown with `invalid_instant`. Higher-resolution and other producer
clock laws remain required work; these profiles do not claim to cover them.

Every new record durably captures either a known source instant, immutable
profile identity, native occurrence ID and original artifact byte span, or an
explicit unknown reason: `no_profile`, `not_json`, `projection_failed`,
`missing_field`, `ambiguous_field`, `wrong_kind`, or `invalid_instant`. Duplicate
matching timestamp pointers are ambiguous even if their lexical values agree.
Older records without this additive catalog field report `legacy_unprofiled`.
They are never reparsed using a newly registered profile. Missing/broken declared
profile references or source-time evidence outside the record are storage failures.
Known instants additionally appear as typed `eventTime` metadata RDF literals
and link their `sourceTimeProfile`; they do not replace `receivedAt` or `expiresAt`.

For example:

```sh
target/release/sparrow SOCKET source-time copied-events /timestamp rfc3339
target/release/sparrow SOCKET records --source copied-events \
  --event-from 2026-09-27T00:00:00Z --event-before 2026-09-28T00:00:00Z
```

`records_search` accepts paired `event_from` and `event_before` bounds, plus its
existing text/field and acceptance-time filters. CLI uses paired `--event-from`
and `--event-before`. Bounds are UTC half-open intervals. Unknown source times
are excluded, with `event_coverage.unknown` counted among live source/acceptance
scope records. `outside_interval` counts known source times outside the interval;
`evaluated` is their shared population. `membership_complete:false` identifies
an incomplete bounded catalog scan, so these counts must not be treated as exact
store-wide totals. Chronological event queries sort by source instant and stable
record ID before applying the row limit. Optional text ranking runs only over
qualified event-time members, with the existing native corpus receipts.

The response explicitly reports `time_basis:event_time`, each row's known time
and supporting spans, and event coverage. With no event bounds it reports
`received_at` and preserves acceptance ordering. Record metadata and context
also expose the captured event-time state. Expiry and pins remain based on
acceptance/retention deadlines, independently of old, future or skewed source
clocks. Cursor pagination and event-ordered context are not yet implemented.

Source-time qualification passed the full `make check` gate: 38 storage
scenarios, five core unit tests, the server admission/cancellation unit test,
50 census tooling unit tests and five tooling gate scenarios, both real-daemon/
MCP scenarios including crash/reopen, seeded census self-tests, and all 17
helper-ledger jobs. No distinct exceptions or unsanctioned isomorphic groups
were added. Native governed stored-view SELECT verified the new `eventTime`
RDF literal and immutable profile link. Strict event-time index requests also
refuse source-clock capture that depended on a failed applicable JSON projection.

Before cutover, the rebuilt client refused an event-time request against the
still-running schema-7 daemon with `UNSUPPORTED` and no stdout result rows,
because that daemon did not honor the requested time basis. The adapter checks
the returned typed time basis rather than accepting a silently ignored filter.

The owned development daemon was updated at approximately **2026-10-07
08:06 UTC**, PID **595446**, discovery schema **8**, using the same private
socket, configuration and `store:46c88e949ccec658600b66c7becd5bb7`. The previous
owned process exited cleanly on SIGINT. Store format and native RDF/text profile
identities remain unchanged. Directory/socket permissions remain 0700/0600.

Live acceptance used only a copied, complete **962-byte first record** from
`~/Active/lillith_soul/inner-core/corrections/2026-09-27.jsonl`, rather than
claiming the entire producer file. Its device, inode, size and modification time
were unchanged around reading and qualification. CLI export matched every copied
byte, SHA-256
`46aa5ddbc5c4f04e9c4bf96533aa0931628ca41b22222891d36ff502ca378ed1`.
No producer file was renamed, deleted or consumed.

The declared source `copied-correction-source-time-v1` uses `/timestamp` and
`rfc3339`, immutable profile
`time-profile:961b463f0ed6a30f7031df16ba0348e82f61c0848196e8ee0046574f8df4ef2e`.
The copied artifact is `artifact:476f80ab8f332b28801d5383902a9c4f`, record
`record:08a1adcc30816e2f98f4c0db4ebc7da1`. Its source instant is
**2026-09-27T00:13:37.406Z**, native occurrence **5**, exact span **[171,195)**;
its independent acceptance time is **2026-10-07T08:06:59.738Z**. CLI and MCP
event-interval queries returned identical rows and complete coverage with one
evaluated record, zero unknowns and zero outside the interval. Adding an
acceptance-before bound of September 28 excluded the record, demonstrating
that event and acceptance filters intersect independently. MCP record metadata
reported the same native event evidence, and the adapter exited cleanly on EOF.

The development store now retains six records, five payload chunks, two shared
JSON projections, no active pins and no pin read handles. This qualification
covers one copied source record; streaming, watcher/producer lifecycle, whole
corpus coverage, source-clock laws beyond this millisecond profile, pressure/
performance acceptance, and full governed SPARQL remain unfinished.

## Bounded unpublished streaming journals

The core now provides `import_begin`, `import_get`, `import_append`,
`import_read_chunk`, `import_seal`, `import_cancel`, and `import_reclaim` as the
storage foundation for streaming imports. The schema-9 daemon and CLI now expose
this path, as qualified below. **Sealing does not publish an artifact or records,
and never authorizes source deletion.**

Beginning an import binds a request identity to the explicit source, format,
complete length and canonical BLAKE3 payload identity. At most 16 journals occupy
the registry; each catalog row is bounded at 8 KiB, including JSON escaping of
source/request labels. The journal contains counters rather than a growing
chunk array. Its fixed 48-hour recovery lease is not extended by activity.

Append accepts one aligned 64 KiB chunk, except for the declared final remainder.
Each synced WAL batch atomically commits the bytes, a native BLAKE3 chunk digest,
and the next offset. Matching retries return the committed journal; changed
bytes, gaps, partial non-final chunks and changed declaration arguments are
refused. Staging uses private payload keys outside evidence references and
never adds searchable records. Disk admission includes staging batches and
preserves the configured maintenance reserve.

Sealing checks every chunk length/digest and computes the declared whole-input
identity with PurRDF's bounded streaming BLAKE3 engine. It does not buffer the
whole file. Cancellation ends the lease durably. Maintenance reclaims at most
128 expired chunk, layout or preparation units per call and atomically advances each journal's cleanup
cursor; canceled/expired inputs cannot be read or resumed during cleanup.
Physical blob/WAL reclamation still depends on native engine maintenance.

Focused qualification reconstructs CRLF, invalid UTF-8, NUL and a final
unterminated fragment after store reopen; verifies retry/conflict behavior,
complete digest refusal, empty input and the 16-journal ceiling; and stages a
130-chunk input larger than the buffered artifact limit. Cleanup resumes after
reopen from its 128-chunk cursor and removes the remaining two chunks. These
are core store-reopen scenarios. An additional owned test writer is killed with
SIGKILL after acknowledging a synced chunk; reopening recovers its exact offset
and bytes, accepts the final remainder and verifies the whole digest. This is
not daemon streaming or completed-import publication acceptance. The subsequent
publication and schema-9 sections qualify those additional boundaries.

The full `make check` gate passed with 41 storage scenarios, including all three
new streaming journal scenarios, release Clippy with warnings denied, all other
workspace tests, seeded census self-tests and all 19 helper-ledger jobs. No
distinct exceptions or unsanctioned isomorphic groups were introduced.

### Private paged record layouts

`import_prepare_layout` now prepares exact source record spans and canonical
record-byte BLAKE3 identities after sealing. It streams the checked raw chunks
through native hashers and retains at most one 64 KiB chunk and one 128-record
page, including for records larger than the buffered artifact limit. Buffered
ingestion and layout preparation call the same raw delimiter implementation.
CRLF remains intact, delimiters belong to their preceding record, and a
completed final unterminated line becomes a record. Empty JSONL/log inputs
have zero records; empty JSON, Markdown and opaque artifacts have one record.

Each private page is at most 32 KiB, carries a checked native content digest,
and commits atomically with the journal's prepared-record counter. The complete
layout flag is committed only after all byte spans and the complete raw digest
have been checked. Record-count or disk-pressure refusals retain private partial
pages and raw staging. A retry rebuilds the deterministic pages without creating
occurrences and preserves previous cleanup coverage. Old raw-only journals
decode with an explicitly unprepared layout.

`import_layout_page` requires a complete layout and an aligned record ordinal,
returns at most 128 spans plus the complete count and next ordinal, validates
source ranges/ordering and contiguous neighboring pages, and uses the same
fixed recovery lease. Missing/damaged pages are storage failures rather than
empty results. Cancellation/expiry cleanup shares its 128-unit budget between
raw chunks and layout pages and resumes both counters after reopen.

Qualification covers CRLF split across chunks, empty records, a binary final
unterminated record larger than the buffered artifact limit, 130 records across
two pages, native record-digest identity, empty inputs for every format, partial
record-limit refusal followed by reopen/retry, and damaged/missing page refusal.
Cleanup removes both raw and layout pages across a bounded restart boundary.
These layouts remain unpublished core preparation. Native record preparation
and final publication are described below. Cap'n Proto streaming caller wiring
is qualified in the schema-9 section below.

The full `make check` gate passed for the layout change with 44 storage
scenarios, all other workspace tests, release Clippy with warnings denied,
seeded census self-tests and all 21 helper-ledger jobs. No distinct exceptions
or unsanctioned isomorphic groups were added.

### Shared record preparation and staged range reads

Buffered ingestion now calls one internal `RecordBatch` to prepare native
JSON occurrence facts, JSON/Markdown RDF, metadata RDF, persisted text,
captured source time and exact raw chunk ownership. Its finalizer writes the
same shared projection/content reference counters and disk-admission estimate.
Catalog publication and retry receipts remain the caller's atomic commit.
Buffered imports retain record-count preflight before native preparation, using
an allocation-free boundary count rather than an artifact-sized boundary vector.
The former inline native preparation body was removed rather than retained as
a second implementation for the future streaming caller.

`import_bytes` materializes exact ranges of committed staging bytes, including
unsealed inputs, at a maximum of 1 MiB per call. It refuses bytes beyond the
committed offset, overflow, expired/canceled journals and excessive output.
It reads only the intersecting checked staging chunks. Staging and ordinary
evidence byte reads share one exact range assembler; input-specific chunk digest
and lifetime checks remain at their respective storage boundaries. Binary
content, cross-chunk ranges and zero-length reads remain exact after reopen.

The shared builder now also serves private native preparation and final
publication described below. Typed daemon caller wiring is qualified below.

The full `make check` gate passed for shared preparation and staged byte reads:
45 storage scenarios, all other workspace tests, release Clippy with warnings
denied, seeded census self-tests and all 23 helper-ledger jobs. Existing native
projection, ranking, exact-byte, pin/retention and daemon crash/reopen scenarios
continue to pass through the shared production preparation path. No distinct
exceptions or unsanctioned isomorphic groups were added.

### Checkpointed native preparation and complete publication

The core adds `import_preparation_get`, `import_prepare_record`, and
`import_publish`. Preparing one expected record ordinal commits its native
JSON/Markdown, metadata RDF, text and normalized exact-byte dependencies in
the shared builder's synced batch, together with its private record catalog
entry and next preparation ordinal. No ordinary record catalog or temporal
membership is written during preparation. Queries, metadata lookup and pins
cannot treat a prepared prefix as published evidence. Retrying an older
ordinal returns existing progress; skipped ordinals are conflicts. Reopen
preserves progress, reserved artifact identity and all dependency ownership.

The first preparation attempt durably captures one server receipt clock and
immutable source-time profile identity (including explicit absence). All
occurrences use that receipt clock and its 30-day ordinary deadline, independently
of the time spent preparing their dependencies. A newly declared source profile
does not reinterpret the already captured preparation context. The final
availability boundary is the atomic publication commit, rather than a claim
that the captured receipt clock is an exact commit timestamp.

Publication checks every private record's provenance, uniqueness, original
span, captured source-time evidence and metadata RDF presence. It streams the
normalized raw chunks through the shared checked payload reader, verifies each
record's native digest against its sealed layout, and verifies the whole
artifact digest and full byte cover. It then commits the complete artifact,
ordered record catalogs, temporal membership, published staging marker and
retry receipt atomically, after the preceding synced dependency batches.
Final metadata plus receipt is bounded to 128 MiB, additionally constrained
by catalog decoding bounds, current record limit and disk admission. A failed
check/refusal does not publish an artifact or a record prefix.

The final publication receipt lasts 48 hours after publication and can be
retrieved after raw/private staging cleanup. Repeated beginning or publication
of the same declared input returns the published identity; conflicting request
arguments fail. Published evidence cannot be canceled as staging. Its normal
retention/pin rules apply. Expired staging cleanup deletes private record copies
without decrementing transferred published references. Abandoned preparation
releases both reserved artifact and record payload ownership, its projection
references, metadata RDF and text through the same retirement implementation as
ordinary expiry. Cleanup admits at most one prepared record within its shared
128-unit chunk/layout/control budget per call, with durable recovery cursors.

Core qualification publishes a file of **16 MiB plus two bytes**, above the
8 MiB fixture buffered limit, as one complete artifact with two records.
Bounded exports reconstruct every byte and native whole-input identity; its
oversized text projections remain explicit failures without losing raw bytes.
A record pin survives parent expiry and releasing it reclaims all remaining
payload references. Further scenarios cover private-prefix invisibility,
native JSON/RDF/source-time capture, reopen and ordinal retries, publication
retry after staging cleanup, canceled shared dependencies, and missing prepared
payload refusal with no published prefix.

This subsection records core publication qualification; typed RPC/CLI and
daemon crash qualification follow below. This initial core implementation
materialized at most **16 MiB per individual record** and refused larger records.
The schema-10 extension below removes that raw-publication refusal. Measured
RSS/disk-pressure and whole-sample acceptance remain required.

The full `make check` gate passed with 50 storage scenarios, including the five
new native publication/retirement scenarios and cleanup-budget exhaustion across
multiple journals. All other workspace tests, release Clippy with warnings
denied, seeded census self-tests and all 26 helper-ledger jobs passed. No
distinct exceptions or unsanctioned isomorphic groups were added.

### Schema 9: typed resumable file imports

`importBegin`, `importAppend`, `importGet`, `importSeal`, `importLayout`,
`importPrepare`, `importPublish`, and `importCancel` are generated Cap'n Proto
RPC methods over the same private Unix socket. References carry persistent
store identity; wrong-store imports fail before storage mutation. Source/request
labels are checked before copying, and append payloads are limited to 64 KiB
before copying into a storage worker. Every storage operation uses the existing worker admission
permit, including its cancellation ownership law.

Beginning returns an explicit union: unpublished staging progress, or the
original accepted artifact. Buffered and streamed ingestion share the same
request receipt and compare source, format, complete length and native BLAKE3
identity. Existing buffered receipts and earlier publication receipts remain
readable through their recorded artifact fields. A buffered request cannot
create another occurrence while its journal exists. Conflicting request reuse
fails; separate requests with equal bytes remain separate occurrences.

The CLI `ingest REQUEST_ID SOURCE FORMAT FILE` now hashes and uploads through a
single 64 KiB buffer, seeks to the daemon's durable offset on retry, seals,
prepares native records by expected ordinal, and requests final publication.
There is no whole-file `read_to_end` path. It verifies the opened regular file's
device, inode, length, mtime and ctime before upload and before publication,
including nanoseconds. A detected change fails without returning an acceptance
receipt; transport failure preserves staging for the stable request retry.
Files are never renamed or deleted by this command. Schema-8 daemons are refused
before staging; use matching executables.

The real-daemon test kills only its owned children after a committed chunk and
a native record checkpoint. Reopened stores preserve the exact offset and
reserved artifact identity. The production CLI resumes preparation and publishes
one complete artifact; retry returns the identical receipt. Bounded byte reads
reconstruct every source byte. Private preparation is inaccessible through
artifact lookup. Another CLI fixture accepts 8,388,609 bytes despite the daemon's
8 MiB buffered-artifact limit. Core tests cover both directions of retry across
the buffered and streamed paths, conflicting arguments, and prevention of a
second occurrence. The client file guard also rejects a same-length rewrite.

These schema-9 checks did not establish oversized individual record publication
beyond 16 MiB. Producer consumption/watchers, whole-sample throughput or RSS limits,
full governed SPARQL, logging-client delivery, or complete architecture acceptance.

The full `make check` gate passed with 51 storage scenarios, two server unit
checks, three real-daemon/MCP scenarios, the remaining workspace tests, release
Clippy with warnings denied, seeded census self-tests and all 30 ledger jobs.
No distinct exceptions or unsanctioned isomorphic groups were added.

On 2026-10-07 the owned development daemon was replaced with the matching
schema-9 release executable. Its endpoint remains
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`, mode 0600, inside
the mode-0700 store directory. Persistent store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`.

A temporary, completed copy of
`lillith_soul/inner-core/corrections/2026-09-26.jsonl` was imported through the
production CLI: 167,222 bytes, 171 records, artifact
`artifact:022887776d4345646bdc2ffc5d4dd406`, payload
`blake3:bfb073b4e6367b07412200888a631546ea24512643a517e64b4df490feae9214`.
The repeated request returned the identical durable receipt; full bounded export
matched every byte; source bytes remained unchanged. Native key discovery for
this source returned 41 rows. This is one-file live acceptance, not whole-corpus
or throughput qualification. No producer file was claimed, renamed or deleted,
and no sibling/model service was managed.

### Schema 10: oversized raw records with bounded preparation

Raw publication no longer requires materializing an entire record. Records above
the 16 MiB projection admission boundary prepare at most 128 normalized 64 KiB
chunks per call. Each synced batch commits payload bytes, shared occurrence
reference increments, one checksum-protected chunk-identity page, and the raw
checkpoint together. Chunk boundaries are relative to the record; a record
starting inside an upload chunk is assembled exactly from the checked source
ranges. The persistent reserved record ID survives reopen and is inaccessible
through public record lookup before complete publication.

`ImportPreparation.raw` is an explicit typed optional value carrying the record
ordinal, reserved record ID, prepared chunk count and complete chunk count.
The CLI advances this durable progress, rejects nonadvancing or inconsistent
responses, and publishes only after every record is complete. Repeating a current
record ordinal advances from its saved raw chunk cursor rather than registering
the previous page twice; an older record ordinal only recovers progress.

After all raw pages are durable, final record preparation streams checked
normalized payloads through native BLAKE3 and compares the exact complete record
length and sealed digest. It then uses the existing `RecordBatch` implementation
for projection state, captured source-time profile, text state and native RDF
metadata. JSON refusals use the native profile's public typed source-byte limit.
Markdown materialization is explicitly limited to 16 MiB; text remains limited
to 1 MiB. Opaque content remains not applicable to those projections. Applicable
limits produce persisted failed diagnostics, not empty successful projections.
The source-time reason for an oversized profiled JSON record is
`projection_failed`; receipt-time retention remains independent.

Buffered records and raw checkpoints share `Writer::prepare_payloads` for
content deduplication and two-owner reference increments. Existing normalized
content must agree with its native digest and positive ownership counter.
Final preparation transfers those references to the private complete record;
publication transfers them to ordinary record/parent evidence without another
increment. Abandoned raw checkpoints retire at most one 128-entry page per
cleanup step through `Retirement::payloads`. Shared content belonging to another
accepted occurrence survives. Completed private records use the same retirement
implementation as ordinary expiry. Layout and raw identity pages share one
bounded native-JSON/checksum codec.

The raw checkpoint is bounded at 8 KiB; identity pages at 32 KiB. A raw chunk
batch holds at most 8 MiB of distinct source bytes before batch encoding. Final
record preparation retains ordered chunk identities, not complete source bytes,
and refuses a manifest exceeding the native catalog string budget. Final
artifact metadata remains limited to 128 MiB and its native decoder limits;
disk admission still reserves maintenance capacity. Those resource refusals
retain private retry/cancellation state and do not publish a prefix. Whole-record
and whole-artifact digest checks perform work proportional to the declared input;
latency, peak RSS, physical reclamation and sustained pressure remain unqualified.

Core scenarios verify a valid 17 MiB JSON record starting at byte three,
128/256/273-chunk checkpoints across reopen, complete artifact/record byte reads,
failed JSON/RDF/text coverage, strict index refusal, captured unknown source time,
record-only pin survival and eventual reference reclamation. Separate scenarios
cover canceled partial preparation sharing content with an existing occurrence,
missing normalized payload refusal without publication, and oversized JSON,
Markdown and opaque document behavior. The real-daemon scenario now resumes an
oversized record after a raw ownership checkpoint and preserves its reserved
record identity through the production CLI and exact export.

This qualifies oversized raw ingestion, not producer consumption/watchers,
logging-client delivery, full governed SPARQL, complete query scopes/pagination,
whole-corpus performance, or full architecture acceptance.

The full `make check` gate passed with 55 storage scenarios, two server unit
checks, three real-daemon/MCP scenarios, all other workspace tests, release
Clippy with warnings denied, seeded census self-tests and all 35 ledger jobs.
There are no distinct exceptions or unsanctioned isomorphic groups.

The owned development daemon now runs the matching schema-10 release executable
at the same private socket and persistent store identity. A synthetic completed
JSONL file containing one 17,825,807-byte record was accepted durably as
`artifact:f462b1c1f32180fb4e77c29194c3677d`, record
`record:5bcf538161b77b607f723b761c66333c`, payload
`blake3:9e453ecc3570700ac96f213aa692d46db7a6b2e27d1e6df898075041246ac32f`.
Stable retry returned the identical receipt; consecutive CLI byte ranges of at
most 1 MiB matched the complete original record. Native RDF inspection reported
the explicit failed source-byte projection, and key discovery reported one
eligible record with one failed JSON projection. This exercises live oversized
raw acceptance and coverage reporting, not throughput or memory qualification.
No producer file or sibling/model service was modified.

## Retained tail range index

Source now fences writers at store formats 33..36. A persistent ordered
`tail-range:` index records generation, source offset, and original record ID.
The shared record publisher adds membership in its record/index batch; the
shared retirement owner removes it with raw-byte dependencies. Generation
metadata alone does not retain expired bytes, and record pins preserve their
original byte ownership.

Startup upgrades historical formats 5..32 through one bounded catalog pager,
shared with the recordless-artifact migration. The writer fence and initial
range checkpoint commit together; every subsequent index page and checkpoint
commit together with synchronized WAL. Startup resumes incomplete pages and
refuses missing or unfenced index state. Backfill adopts existing retained
record rows, preserving occurrence IDs, acceptance clocks, finalization
receipts, and deadlines. It does not reread source files or restore retired
chunks.

This is the ownership/index foundation for retained-file assembly. A complete
file reader and completed-tail claim/deletion handoff remain unfinished. The adoption evidence below records the current deployed build.


## Retained-range daemon adoption

The retained-range adoption ran `sparrow-development.service` PID **469821**, started at
2026-10-08T11:08:06Z, from signed functional source **cf924de**. Its executable
is `/home/paudley/.local/libexec/sparrow/sparrowd-schema39-cf924de`, SHA-256
`61075ed429ba5c57094ef5de2c8f5664656cc7060431e910c59ff39c5e4d2b90`,
with sole capability `cap_net_bind_service=ep`. It listens on **0.0.0.0:518**
and the existing private Unix socket. Matching installed CLI and events example
names use `schema39-cf924de`; their hashes match the original schema-39 clients
above because their source did not change. All three installed hashes were
compared with the explicit all-bin/example build after the signed hook passed.

Store `store:46c88e949ccec658600b66c7becd5bb7` migrated from format 32 to 36.
CLI/MCP receipt equality, exact-byte reads, provenance/context, and SDK public
TCP/TCP/Unix retries passed with **210 records**, six tail checkpoint records,
one active held generation, and no tail admission failure. No new occurrence
or source removal was part of this adoption. The closed pre-upgrade snapshot is
`/tmp/sparrow-tail-range-live.2qmojs6w/store-before-range-upgrade`; restoration
was not exercised. Live acceptance evidence, hashes, service state, before/after
status, and request/response captures are in that same directory.

The old unit's default stop signal released its PID and TCP endpoint but left a
stale Unix socket. After confirming the unit inactive, PID absent, and connection
refused, that owned socket was removed before the new process started. The new
unit explicitly uses `KillSignal=SIGINT`, the signal handled by the daemon's
shutdown path. This remains a transient development unit; permanent deployment
packaging is unfinished.

The normal signed hook passed release Clippy with warnings denied, 136 storage
scenarios, 48 RPC scenarios, remaining workspace tests, seeded census self-tests,
and the pinned cross-workspace gate: **201 jobs, no distinct exceptions**.
Its first attempt exposed a transport-fixture capacity race: four concurrent
syslog publishers could occupy all four permits while status was polled. The
fixture now admits five operations for four publishers plus its observer;
production admission limits and all packet/counter assertions are unchanged.
The focused scenario and final full hook passed. Evidence:
`/tmp/sparrow-tail-range-syslog.log`,
`/tmp/sparrow-tail-range-commit-final.log`, and
`/tmp/sparrow-tail-range-bins.log`. Complete retained-file assembly and
completed-tail claim/deletion handoff still require implementation.


## Schema 40 retained whole-file reads

Source adds a typed file-generation branch to the existing byte RPC. SDK,
CLI and MCP reuse one certified byte request/response path. The core serializes
full-cover validation against pins and retirement, streams bounded range catalog
rows, and uses the existing digest-checked chunk reader for requested bytes.
It neither reads source paths nor reimports occurrences. Missing or ineligible
cover refuses even a request for surviving final bytes; those records remain
independently readable. Original acceptance and expiration clocks are preserved.

Core scenarios cover multi-batch and cross-chunk exact reconstruction, CRLF and
final unterminated bytes, full-cover pins followed by an independently pinned
last record, expired but unretired prefixes, retired prefixes, reopen, empty-file
expiry, foreign index membership and hard streaming-row admission. The actual
TCP/Unix daemon scenario exercises SDK/CLI/MCP reads, store/schema refusal,
completion retries, source replacement/removal and reopen. SDK receipt fixtures
refuse foreign stores and both short and long replies. The interface document
records exact finite admission ceilings and per-read eligibility semantics.

Completed-tail claim/deletion handoff remains unfinished. The live adoption
below identifies the installed functional build and its acceptance evidence;
this source section alone is not deployment evidence.

## Schema 40 daemon adoption and whole-file acceptance

The schema-40 `sparrow-development.service` adoption ran PID **723224**, started at
2026-10-08T11:39:02Z, from signed functional source **d2bdd98** plus the SDK byte
example commit **fb49020**. Its immutable executable is
`/home/paudley/.local/libexec/sparrow/sparrowd-schema40-fb49020`, SHA-256
`8dab0ebedb09bab915cde103f3212284e8cc2be138bb40298543bb49c900b91e`.
The sole capability is `cap_net_bind_service=ep`, and the public Cap'n Proto
listener remains **0.0.0.0:518**. The unit keeps `KillSignal=SIGINT`; the prior
process exited and removed its Unix socket before the closed snapshot and reopen.
This is still a transient development unit; permanent packaging remains unfinished.

The matching installed clients are:

- CLI `sparrow-schema40-fb49020`, SHA-256
  `0b3bbf00afffddc87f1f6defc4f5de57a2982cd25107a382ce89af6d0cb66e7f`.
- SDK events `events-schema40-fb49020`, SHA-256
  `258795190a38d901bb6893dd01a40f89b1635f9fe489417507fd38d9fc0a75b4`.
- SDK byte query `bytes-schema40-fb49020`, SHA-256
  `8e1fdba04141cfc02ed248720ab34c65b01c769b9cc00952b2dd6dfcba827d5c`.

All four installed hashes matched the explicit all-bin/example build after the
signed hooks passed. Store `store:46c88e949ccec658600b66c7becd5bb7` retains
format **36**, **210 records**, six tail checkpoint records and one active held
generation, without an admission failure. No new occurrence or source removal
was part of this adoption.

SDK byte queries over **192.168.2.4:518** and Unix reconstructed the exact
107-byte rotated generation and 52-byte explicitly completed partial-JSONL
generation. Both were compared with owned source observations saved before the
cutover. Cross-batch ranges and exact EOF zero-length reads also agree. CLI
file exports and MCP base64 reads returned the same complete original bytes.
The unfinished active generation refused the public SDK read with
`EVIDENCE_UNAVAILABLE` and no byte prefix. The original CLI/MCP completion
receipts, per-record byte reads and provenance/context remain unchanged; current
SDK TCP/TCP/Unix event retries preserve the original schema-38 artifact.

Live captures, binary identities, before/after status, original source bytes,
MCP transcripts and service state are in `/tmp/sparrow-schema40-live.eqio9pik`.
The closed snapshot is its `store-before-schema40` directory; restoration was
not exercised. `verify.py` performs reads and exact admitted retries;
`whole_files.py` performs reads and exclusive local exports. Neither creates
intake occurrences or removes source files.

Normal signed hooks passed release Clippy with warnings denied, **138 storage**
scenarios, **48 RPC** scenarios, **15 SDK** tests, remaining workspace tests,
seeded census self-tests, and the pinned cross-workspace gate: **205 ownership
jobs, no distinct exceptions**. Evidence is in
`/tmp/sparrow-file-bytes-commit.log`,
`/tmp/sparrow-file-bytes-example-commit.log`, and
`/tmp/sparrow-schema40-bins.log`. Completed-tail claim/deletion coordination and
full architecture acceptance remain unfinished.

## Completed-tail claim storage foundation

The core consumption journal now supports retiring a producer-finished tail
generation without importing its accepted bytes again. An explicit handoff
names the original generation; admission loads its actual durable terminal
checkpoint and requires a complete, gap-free producer finish, the original
watch revision, source interpretation, root identity, and exact final descriptor
observation, including nanosecond modification and change clocks. A path rotated
before the producer finish may be handed off under its current relative path.
A changed final observation is refused.

The entire original checkpoint is included in reservation identity and persisted
with source ownership. It keeps the original declaration, record count, byte
prefix, completion admission/publication times, and metadata deadline. Pending
ownership can therefore recover after the bounded generation registry expires.
Acknowledging the claimed checkpoint publishes its existing generation identity;
it creates no artifact, record, payload, pin, or new acceptance clock. Uploads
using this reserved consumption identity fail rather than duplicate records.
Source removal confirmation and the completed 48-hour retry lease use the same
consumption journal and handoff binding as ordinary completed-file imports.

The first such reservation atomically selects `sparrow.store/37`, which older
writers refuse. Ordinary stores continue to use formats 33–36; this change does
not migrate or rebuild their catalog. Watch updates and subsequent ordinary
handoffs preserve format 37. A tail journal under an older format fails when
read, and altered or missing checkpoint proof fails its reservation identity.
Reservations leave 512 bytes under the 8 KiB journal ceiling for the subsequent
claimed observation, removal clock, and completed receipt wrapper.

The storage foundation was committed as `c68cc36`. At that commit public
RPC/SDK/CLI/MCP handoff selection remained unfinished. Filesystem-worker integration is recorded
below. The running schema-40 development service has not been changed by this
work.

Qualification includes a release storage scenario covering incomplete-finish
refusal, exact-clock/revision/root conflicts, checkpoint tampering and missing
publication state, a downgraded writer fence, reopen before publication,
generation-registry expiry, import refusal, unchanged record clocks, absence of
expired-record resurrection, removal retries, and subsequent ordinary handoffs.
The repository gate passed with 139 storage scenarios, 48 RPC scenarios, 15 SDK
tests, remaining workspace tests, seeded census self-tests, and the pinned cross
gate with 206 ownership jobs and no distinct exceptions. Gate and normal signed
commit verification logs are `/tmp/sparrow-tail-claim-check.log` and
`/tmp/sparrow-tail-claim-commit.log`.

## Completed-tail filesystem retirement worker

The consumption worker now recovers and retires sealed completed-tail claims
through the existing `ClaimRoot` namespace, original descriptor validation,
no-overwrite move, directory synchronization, and deletion recovery. Its tail
owner lazily creates that same claim coordinator from a clone of the already
admitted root descriptor, without reopening the configured path. A retain watch
alone still creates no private claim directory and authorizes no deletion.

Ordinary inputs use the existing typed uploader. A terminal tail claim instead
acknowledges its sealed checkpoint directly, without connecting to an upload
socket. Both paths share claimed-descriptor validation and removal. A published
tail whose private source was already unlinked is recovered through the same
directory-sync and removal-confirmation path. Tail acknowledgement and retirement
require a permit from the shared daemon operation budget. Ready tail claims take
precedence over ordinary upload connections, and a tail failure does not discard
an unrelated upload connection.

Admission requires a tail-generation target for active-file watches and ordinary
handoff for completed-file watches. At worker commit `590c09a` the public intent
codec could not yet name the tail target; public wiring and live adoption were
unfinished. That worker commit was not independently deployed.

A release acceptance scenario runs the actual worker without any RPC listener,
recovering reserved, claimed, published, and already-unlinked sources. It verifies
operation-budget blocking, private-directory laziness, original record/checkpoint
clocks, no duplicate occurrences, preserved replacement paths, and hard refusal
of a late writer mutation with the original bytes and journal still owned.
Focused evidence is `/tmp/sparrow-tail-worker-focused.log`; repository and signed
commit verification logs are `/tmp/sparrow-tail-worker-check.log` and
`/tmp/sparrow-tail-worker-commit.log`.

The repository gate passed with 139 storage scenarios, 49 RPC scenarios, 15 SDK
tests, remaining workspace tests, seeded census self-tests, and the pinned cross
gate with 208 ownership jobs and no distinct exceptions. Public tail handoff
qualification and live deployment remain unrun.

## Schema 41 typed completed-tail handoff

The public Cap'n Proto intent, SDK, CLI and MCP now expose completed-tail source
retirement. The immutable intent adds an optional original generation target.
The receipt separately certifies its existing generation publication and leaves
the artifact identity absent; ordinary handoff keeps its original artifact
semantics. Intent files use the same bounded native codec and historical ordinary
files decode with no tail target. CLI preparation checks the original producer
finish against the current exact descriptor observation before saving an intent.
Both MCP handoff operations share the CLI's SDK call and renderer, and producer
finish/source handoff share one strict native JSON file-observation decoder.

The Rust `handoff` example admits or reads a saved intent through the same SDK
over Unix or public TCP, printing the certified typed receipt. It accepts
`handoff admit|get ENDPOINT INTENT.capnp`. The MCP catalog now contains 30 tools;
handoff mutation is marked destructive and lookup is read only.

A real-daemon acceptance scenario admits the original finished tail over public
TCP, reads exact retained bytes after source retirement, replays the saved intent
through SDK, CLI and MCP, preserves a replacement source, and recovers the same
receipt/records/checkpoint after reopening the store. It refuses preparation
before producer completion, a missing tail target, and conflicting retry targets.
The SDK additionally checks all receipt phases and refuses wrong or competing
artifact/tail publication identities. Focused evidence is
`/tmp/sparrow-schema41-focused.log`; repository and normal signed commit gates
are `/tmp/sparrow-schema41-check.log` and `/tmp/sparrow-schema41-commit.log`.

These changes were committed as `d535b1a`; live adoption is recorded below.
Full architecture acceptance remains unfinished.

## Schema 41 daemon adoption and public tail retirement

The current `sparrow-development.service` runs PID **1157669**, started at
2026-10-08T12:34:36Z, from signed source **d535b1a**. Its immutable executable is
`/home/paudley/.local/libexec/sparrow/sparrowd-schema41-d535b1a`, SHA-256
`762e88469f82fdebe939b734c2ecd97b6299d05be2f1046445542ee02d38553d`.
Its sole capability is `cap_net_bind_service=ep`; public Cap'n Proto remains
**0.0.0.0:518**, with LAN acceptance through **192.168.2.4:518**. The unit uses
`KillSignal=SIGINT`, working directory `/home/paudley/Active/sparrow`, and
`UMask=0077`. The prior process exited and removed its private socket before
the closed snapshot and reopen. This remains a transient development unit.

Matching installed clients under `/home/paudley/.local/libexec/sparrow` are:

- `sparrow-schema41-d535b1a`, SHA-256
  `bfefd120759f976a5f2c236c187f5121f1f55f14c894382d3c6b8603dfaea422`.
- `events-schema41-d535b1a`, SHA-256
  `3354982a6100facce24d65049abe408b3bb51e785ef78f2c7b4b899835962a85`.
- `bytes-schema41-d535b1a`, SHA-256
  `ebd880f1982948dbae44e616b9eeabfb86c40e1ef2f2f94d6aeb2479eccfdcab`.
- `handoff-schema41-d535b1a`, SHA-256
  `dd22c5a9e208dba91d3095a438cbf3819fe609e94734dc6065d0e2f0fc2f2995`.

All installed hashes matched the explicit all-bin/example build after signed
hooks passed. Before new intake, live verification preserved the original store
identity, format 36, 210 records, old completion receipts and exact bytes,
record provenance/context, and original schema-38 event artifact
`artifact:6a81f30ff545ca344c0d579cbffcfdce`. Old complete files still reconstruct
exactly over SDK TCP/Unix, CLI and MCP; active generation reads still refuse
without returning a byte prefix.

One exclusively created synthetic file in the owned development tail root
contained 92 bytes, including a final unterminated JSONL record. Preparation
before producer finish refused without creating an intent or deleting its source.
After its original producer completion, the SDK handoff example admitted the
saved intent through the LAN TCP endpoint. The worker retired only that original
source and published generation
`file-generation:238fc933ad32a1a5f95004cd5c6bb1db`, with no artifact identity.
Original finish admission/publication remain 2026-10-08T12:37:07.256Z and
2026-10-08T12:37:07.310Z. The complete 92 bytes remain readable over TCP/Unix.

An independent replacement at the original path remains intact. SDK TCP/Unix,
CLI and MCP mutation/progress retries all return the same removed receipt and
preserve post-retirement record clocks, checkpoint metadata, and replacement
bytes. Store `store:46c88e949ccec658600b66c7becd5bb7` now uses format **37** and
has **212 records**: exactly the two new checkpoint records, with no reimport.
The tail watch has eight checkpoint records, two active held descriptors, no
pending claim or failure, and no admission wait; pins and record-read handles
remain zero. Post-handoff verification also preserves all older completion and
event retry evidence. No production or Lillith source was watched or consumed.

Evidence is `/tmp/sparrow-schema41-live.nts9vrel`: installed artifact hashes,
before/after status, original synthetic bytes, native retry intents, receipt,
checkpoint and record captures, public SDK output, MCP transcripts, and service
state. Its `store-before-schema41` is a closed snapshot; restoration was not
exercised. `handoff_live.py` creates/adopts one synthetic source and must not be
rerun against this accepted fixture. `handoff_retry_verify.py` and the two
`*_post_handoff.py` verifiers perform reads and exact admitted retries only.
The initial collector stopped after successful retirement on JSON-field/type
assertions; those collection errors were repaired, and verification continued
using the original saved intent without reingestion or service restart.

Normal signed hooks passed formatting, release Clippy with warnings denied,
139 storage scenarios, 50 RPC scenarios, 16 SDK tests, remaining workspace tests,
seeded census self-tests, and the pinned cross gate: 210 ownership jobs with no
distinct exceptions. Logs are `/tmp/sparrow-schema41-commit.log` and
`/tmp/sparrow-schema41-bins.log`. Permanent packaging and full architecture
acceptance remain unfinished.

## Unix and public TCP agent adapters

The CLI and stdio MCP adapter now accept the SDK's shared endpoint spelling:
a Unix socket path or `tcp://IP:PORT`. CLI operations and individual MCP calls
use `Client::connect_endpoint`, which validates discovery and owns the same
generated capability/connection lifetime for either transport. The endpoint
parser remains in the SDK; no second address parser or query path was added.
MCP retains bounded framing, four in-flight calls, cancellation and its existing
tool deadline. Network refusal remains a typed `RPC_FAILURE` tool result.

The existing release RPC/MCP acceptance harness now accepts either endpoint
through one shared home. A real-daemon TCP scenario runs its full evidence,
temporal, text/field/value, RDF inspection, context, pin and mutation scenarios,
compares current record rows/store identity with Unix access, and verifies bad
numeric endpoints and a reaped server refuse without successful output. No wire
schema or storage format changed. Focused evidence is
`/tmp/sparrow-agent-tcp-focused.log`; repository and normal signed commit gates
are `/tmp/sparrow-agent-tcp-check.log` and `/tmp/sparrow-agent-tcp-commit.log`.

The verified client from signed source `9406d95` is installed at
`/home/paudley/.local/libexec/sparrow/sparrow-schema41-9406d95`, SHA-256
`7f57ba0de3c036b8e542ddf956345ab1e5c913bbcb229d1686a1f291dcbab532`.
It was explicitly rebuilt after the commit hook, installed with exclusive
creation, and compared byte-for-byte with the selected release artifact.
The build log is `/tmp/sparrow-agent-tcp-bins.log`.

Live evidence is `/tmp/sparrow-agent-tcp-live.scuso56c`. Its `verify.py` uses the
installed CLI and stdio MCP over `tcp://192.168.2.4:518`, reusing the saved
schema-41 native retirement intent. Reads and exact admitted retries preserve
the original receipt, 92 source bytes, terminal clocks and replacement file.
Text, key, decoded-value, temporal and record rows agree between the installed
CLI's LAN TCP and Unix endpoints. The temporal interval selects the two original
retirement records; text and decoded-value searches each select one record.
The store remains `store:46c88e949ccec658600b66c7becd5bb7`, schema 41,
format 37, with 212 live records, no pins or read handles, and no watch failure.
Daemon PID 1157669 and its installed image are unchanged; no restart or new
source intake was needed. The full and signed-hook gates passed 139 storage
scenarios, 51 RPC scenarios, 16 SDK tests, remaining workspace tests and census
gates with 210 ownership jobs and no distinct exceptions.

For remote agent access, run the installed client with the TCP endpoint followed
by `mcp --stdio`, or a CLI command such as `status`. The daemon continues to bind
`0.0.0.0:518`. Permanent service packaging and full architecture acceptance,
including the documented upstream stored-view SPARQL integration gaps, remain
unfinished.

## Persistent service adoption

Signed packaging source 9e6fc41 adds the persistent user unit and deployment
contract. The installed unit is /home/paudley/.config/systemd/user/sparrow.service;
its ExecStart uses /home/paudley/.local/libexec/sparrow/sparrowd and
/home/paudley/.config/sparrow/server.toml. The daemon symlink retains the qualified
schema-41 image sparrowd-schema41-d535b1a, SHA-256
762e88469f82fdebe939b734c2ecd97b6299d05be2f1046445542ee02d38553d,
with its sole cap_net_bind_service=ep file capability. The installed CLI symlink
/home/paudley/.local/bin/sparrow selects sparrow-schema41-9406d95, SHA-256
7f57ba0de3c036b8e542ddf956345ab1e5c913bbcb229d1686a1f291dcbab532.
No runtime source or wire schema changed in this packaging step.

The installed configuration preserves the existing database, public binding,
private socket, budgets and synthetic intake. Its only changed field is the
lexicon bundle path, now under /home/paudley/.local/share/sparrow/lexicons-
e58974e504b9681597faea1490002ce8ddaf7448/artifacts (one continuous path).
Artifacts, provenance, collision evidence, notices and licenses were copied from
the exact census archive. Every copied file was compared byte-for-byte with that
archive; the five CBOR artifacts also match the previous live configuration's
inputs. The existing daemon resolver certifies them, and its text-analyzer
fingerprint remains e5533b9df7c75ab405290d27872c6392515acb3d0d77d21d4433ec4c59904058.
Runtime startup no longer depends on the sibling checkout or repository cwd.
Configuration directory/file modes are 0700/0600; the service retains UMask 0077.

The installed unit passed systemd-analyze --user verify. The initial repository
unit verification identified its not-yet-installed executable alias; verification
passed after installing that alias. The old sparrow-development.service was
stopped through SIGINT; PID 1157669 was confirmed terminal and its owned Unix
socket absent before taking a closed-store snapshot and starting the new owner.
The persistent unit started on 2026-10-08T13:02:33Z as PID 1452634, and is enabled
in default.target.wants. Linger=yes was already configured and remains enabled.
Boot configuration is verified; no host reboot was performed. The unit uses
SIGINT drain, a 120-second stop limit and explicit failure without automatic
restart after a crash or failed open.

Live evidence is /tmp/sparrow-persistent-service-live.2whydbvj: before/after status,
unit and process invocation, installed image/configuration hashes, lexicon hashes,
listener observation, native intents, exact byte reads, CLI/MCP retry receipts,
query comparisons and a closed-store-before-cutover snapshot. Store identity
store:46c88e949ccec658600b66c7becd5bb7, schema 41, format 37, 212 live records,
213 payload chunks and 208 JSON projections survived. Profiles match, the
session identity changed, and there are no pending buffered writes or watch
failures. Public TCP and Unix text/key/value/temporal/record rows agree with
pre-cutover evidence. The saved native tail handoff still returns its original
receipt through CLI/MCP and SDK retries, preserving its 92 source bytes,
terminal clocks and replacement file. No new source intake was performed.

The collector initially attempted to resolve the capability-bearing process's
protected executable link. That observation was unavailable; collection
continued through the configured systemd invocation, process arguments, verified
installed alias/image and live service evidence. It did not trigger a restart.
The closed-store snapshot has not been restored in a recovery exercise.

Repository and normal signed-hook gates passed 139 storage scenarios, 51 RPC
scenarios, 16 SDK tests, remaining workspace tests and seeded/pinned census gates:
210 ownership jobs, no distinct exceptions. Logs are
/tmp/sparrow-persistent-service-check.log and
/tmp/sparrow-persistent-service-commit.log. Full architecture acceptance remains
unmet, including full governed stored-view SPARQL, native forced reclamation and
whole-corpus performance/resource/recovery qualification.

## Isolated closed-store restoration qualification

The recovery example in crates/sparrow-server/examples/recovery.rs opens an
explicit restored copy through ServerConfig and Store. It never starts listeners,
watch workers or maintenance. Its one coordination path uses the existing native
byte reader, PurRDF streaming BLAKE3, native JSON renderer and RDF inspection.
It admits 1..1024 distinct explicit evidence targets and at most 1 GiB of reads,
checking target lengths before reading with 1 MiB calls. Complete artifact
hashes must agree with their original catalog payload identities. Failure exits
nonzero with no successful report prefix. The ownership ledger now has 211 jobs
and no distinct exceptions; no second parser, hash law or projector was added.

Evidence is /tmp/sparrow-restoration-live.xy2ws3bp. The closed-store snapshot
from persistent service adoption was copied to restored-store inside that
separate directory. Only the copied configuration's database/socket paths were
changed. The original backup, active database and source roots were not modified.
A fixed eligibility instant of 2026-10-08T13:02:33Z was used. All continuation
pages were followed to capture 212 unique records and 32 complete artifacts,
plus three producer-finished file generations. Every restored target's exact
length and streaming digest matches independent SDK public TCP reads: 247
explicit targets and 36,005,499 aggregate bytes (overlapping artifact/record
bytes counted for each check). All 212 native RDF inspections match live TCP
inspection identities, states and stream counts after excluding capture clocks.
The report preserves store:46c88e949ccec658600b66c7becd5bb7, format 37, 212 live
records and the existing native semantic fingerprints.

Eight refusal cases cover wrong store identity, duplicate or untyped targets,
unfinished generation, expired evidence, live-store ownership conflict, target
count overflow and missing blob evidence in another disposable restored copy.
Each exits nonzero with actionable stderr and empty stdout. The initial fault
collector removed one blob and checked a small inline record; that selected
record remained valid. The fault exercise was corrected to remove blob files
only in its disposable copy and inspect the complete evidence target list,
which then reports the missing blob. The probe is intentionally explicit about
selected evidence and does not claim an engine-wide integrity scrub. Initial
artifact collection also needed a second continuation page; complete collection
followed the native cursor instead of treating the first page as all evidence.

The active persistent daemon remains PID 1452634. No service stop/restart, source
handoff, new intake or original-source mutation was needed for this restoration
exercise. The historical snapshot-not-restored statements above describe the
state before this separate qualification. Successful selected-evidence restore
does not establish whole-process crash injection at every consumption boundary,
query-semantic recovery for all operators, host reboot acceptance or whole-corpus
resource/throughput qualification. Full architecture acceptance remains unmet.

Focused release Clippy and the full repository gate passed. Logs are
/tmp/sparrow-recovery-lint.log, /tmp/sparrow-recovery-build.log and
/tmp/sparrow-recovery-check.log. Qualification used the explicit selected example
build, not an unselected volatile target. Installed signed-image qualification
follows.

Signed source 65d68b9 passed the normal commit hook; its log is
/tmp/sparrow-recovery-commit.log. The example was explicitly rebuilt after the
hook and installed with exclusive creation at
/home/paudley/.local/libexec/sparrow/recovery-schema41-65d68b9, SHA-256
22904f6331385572b1feb1690efa7774858e1e59643b2bc5de211e15c31bba1f.
The installed image and selected release artifact were compared byte-for-byte;
the build log is /tmp/sparrow-recovery-installed-build.log. The installed image
reproduced all 247 target rows, 36,005,499 byte checks, native RDF inspections
and eight refusal cases against the disposable restored copies. Its reports
and image identity are recorded as installed-recovery-report.json,
installed-refusals.json and installed-artifacts.json in the same evidence
directory. Final live status remains schema 41 with 212 records and daemon
PID 1452634. This offline tool requires no daemon image change or restart.

## Native half-open temporal query bounds

The shared live query-candidate iterator now uses the existing inclusive forward
seek together with RocksDB's native exclusive `iterate_upper_bound` at
`received_before`. Record, native text and JSON key/value queries therefore do
not decode the first catalog record beyond their requested interval. Selected
membership keeps its existing explicit-reference validation and finite seeks.
No query semantics, wire schema, storage format or dependency pin changed.

The regression fixture corrupts catalog encodings both before the inclusive
start and exactly at the exclusive end. The earlier implementation failed a
valid `[20,30)` record query by loading the damaged record at 30. The corrected
implementation returns its one valid in-interval record and native text/key/value
results. Widening either end to include damaged evidence still produces
`STORAGE_FAILURE`, with no swallowed catalog error. This changes which data a
query reads, not its integrity requirements for selected input. The shared
query-candidates ledger job owns the iterator; no second traversal helper was
introduced.

Before/after focused evidence is `/tmp/sparrow-temporal-bound-before.log` and
`/tmp/sparrow-temporal-bound-after.log`. The full repository gate is recorded in
`/tmp/sparrow-temporal-bound-check.log`. Runtime adoption will use the persistent
unit, existing store, native installed lexicons and the schema-41 client after
signed-hook qualification and an explicitly selected daemon build. Full
architecture acceptance remains unmet; native forced reclamation and full
SPARQL still require the documented upstream seams.

Signed source `008cd78` and its normal hook passed formatting, release Clippy,
140 storage scenarios, 51 RPC scenarios, 16 SDK tests, remaining workspace
tests and census gates: 211 ownership jobs, no distinct exceptions. The hook
log is `/tmp/sparrow-temporal-bound-commit.log`. After that hook, the daemon was
explicitly rebuilt with `--bin sparrowd`; its build log is
`/tmp/sparrow-temporal-bound-bins.log`. The installed image is
`/home/paudley/.local/libexec/sparrow/sparrowd-schema41-008cd78`, SHA-256
`cd6aca0d425ffb4b57f401bf07538bdaf95517052979548aaf3d482eb834513b`.
Its bytes match the selected release artifact. Its sole file capability remains
`cap_net_bind_service=ep`; the stable daemon alias now selects this image.

Live evidence is `/tmp/sparrow-temporal-bound-live.r0ho_rx2`: before/after unit
and status, closed-store snapshot, image identity, configured process invocation,
listener observation, exact saved handoff retries and query comparisons.
The persistent unit stopped PID 1452634 through SIGINT; its Unix socket was
confirmed absent before snapshotting and switching the alias. The new owner is
PID 1710023. Store identity, schema 41, format 37, native profiles, 212 records,
213 payload chunks and 208 JSON projections are unchanged. TCP/Unix
record/text/key/value/temporal rows match prior evidence, and saved CLI/MCP/SDK
tail-retirement retries preserve their original receipts, bytes, clocks and
replacement file. The public listener remains `0.0.0.0:518`, and watches report
no failure or pending claims. No new intake or source mutation was needed.
The new closed-store snapshot has not separately been restored; the earlier
isolated restoration qualification remains recorded above. Full architecture
acceptance is still unmet.

## Copied-corpus disk baseline

[Performance baseline](performance-baseline.md) records the first selected
sample import/query measurements on Btrfs/NVMe using the installed images,
without changing the production unit or its 212 records. Two independent
occurrence passes imported 1,157,768 bytes into 7,058 records, and stable retries
preserved all receipts. Native inspection covers every record. The broad
chronological capture refused at its real byte ceiling; repeated multi-page
queries also encountered retained lease budgets. Successful single-page
queries, positive exact value search, sampled RSS and closed-store file
components are measured separately from these refusals. This evidence directs
the next query-lifecycle work and does not establish full performance acceptance.
Signed evidence source `49923fe` passed the normal full hook; its log is
`/tmp/sparrow-performance-baseline-commit.log`. No daemon rebuild or lifecycle
change was needed for this measurement/documentation step.

## Explicit result lease release

Schema 42 adds `resultRelease` to the typed service, `Client::release_result`
to the SDK, `result-release STORE_ID SNAPSHOT_ID` to the CLI, and
`result_release` to MCP. One generic registry removal serves record, artifact,
source and pin result captures. Consumers can release a completed investigation's
replay memory immediately rather than waiting for expiry. Earlier page cursors
remain replayable until release or expiry; releasing a lease leaves evidence,
pins and retention unchanged. Single-page results already avoid retained leases.

The existing core and RPC result-family scenarios now exercise explicit release,
repeat release, foreign-store rejection and continuation refusal after release.
The core scenario also checks twenty successive captures and releases at one
clock value without raising budgets. SDK certification checks exact receipt
identity and both release outcomes. The earlier measurements remain schema 41
evidence; the schema 42 follow-up is recorded separately.

Signed source `b53f163` passed the normal full hook: formatting, release Clippy
with warnings denied, 140 storage scenarios, 51 RPC scenarios, 17 SDK tests,
remaining workspace tests, seeded census tests and 213 ownership rules with no
distinct exceptions. Logs are `/tmp/sparrow-result-release-check.log` and
`/tmp/sparrow-result-release-commit.log`. The explicit all-bin/example rebuild
after the hook is `/tmp/sparrow-result-release-bins.log`.

The installed daemon is
`/home/paudley/.local/libexec/sparrow/sparrowd-schema42-b53f163`, SHA-256
`23b585c7ad931842f9b742f97801de6074291e050d0d0faad6e75bbc970c2dd6`.
Its sole capability is `cap_net_bind_service=ep`. The stable CLI selects
`sparrow-schema42-b53f163`, SHA-256
`44e530ac5966c70a5112191775ffe9f71ef2c2f64ae1486a39e9e23bd0007b7c`.
Matching events, bytes and handoff SDK examples are installed beside them;
their image hashes are in the live evidence manifest. All installed bytes match
the explicitly selected release artifacts.

Evidence is `/tmp/sparrow-result-release-live.8o4x3zos`. The persistent unit
stopped PID 1710023 through SIGINT; its socket disappeared before a closed-store
snapshot and stable alias replacement. The new owner is PID 2062025, serving
schema 42 at `0.0.0.0:518` and the existing private Unix socket. Store identity
`store:46c88e949ccec658600b66c7becd5bb7`, format 37, 212 records and existing
watches are preserved. TCP and Unix live tests verify exact cursor replay,
wrong-store and cursor-as-snapshot rejection, CLI/MCP release, true then false
receipts, `RESULT_EXPIRED` after release, and immediate return to zero handles
and bytes. Pins and record counts stay unchanged. The schema 41 CLI refuses
discovery against schema 42 as required.

Saved public handoff retries through CLI/MCP/SDK preserve original receipt,
92-byte generation contents, clocks, checkpoints and the replacement file.
No new intake or producer mutation was needed. The new closed snapshot has not
separately been restored; the earlier restoration qualification remains above.
The [disk follow-up](performance-baseline.md#explicit-release-follow-up) shows
ten successful source and temporal traversals each without retained-lease
pressure; the broad capture ceiling and full architecture gaps remain unmet.

## CLI evidence protection parity

The CLI now exposes `pin REQUEST_ID STORE_ID NAME DAYS EVIDENCE_ID...` and
`pin-release REQUEST_ID STORE_ID PIN_ID`. CLI and MCP use one typed mutation
owner rather than duplicating request construction or receipt rendering.
Existing RPC adapter acceptance compares original MCP pin/release receipts with
CLI stable retries, checks wrong-store, zero-duration, malformed-target and
conflicting-request refusals, and retains the shared renewal checks. These
operations use the existing schema 42 daemon contract and need no store migration.

Signed source `b17f121` passed the normal hook, including 140 storage scenarios,
51 RPC scenarios, 17 SDK tests, remaining workspace tests and 214 helper rules
with no distinct exceptions. Gate and hook logs are `/tmp/sparrow-cli-pin-check.log`
and `/tmp/sparrow-cli-pin-commit.log`. The CLI was explicitly rebuilt after the
hook (`/tmp/sparrow-cli-pin-bin.log`) and installed as
`/home/paudley/.local/libexec/sparrow/sparrow-schema42-b17f121`, SHA-256
`9567c010d0e985e52a5ec3c0a07a8b802e7fa9c1f6e4e2c1f176454372681e96`.
The stable CLI alias selects those verified bytes. The daemon remains the
schema 42 `b53f163` image at PID 2062025; no restart was needed for this client
addition.

Public TCP live evidence is `/tmp/sparrow-cli-pin-live.xfx36zzh/report.json`.
The CLI created a one-day pin for one existing owned synthetic tail record,
MCP replay returned the same durable receipt, the CLI released it, and MCP
replay returned the same release receipt. Replaying the original create receipt
after release did not resurrect protection. The final store still has 212 records
and zero active pins, with unchanged schema, format, store identity and unit PID.
Only finite pin control journals were added; no producer file or intake changed.

The [disk crash qualification](performance-baseline.md#disk-crash-recovery)
checks durable pin/import/release boundaries on a separate closed-store clone.
It passed two SIGKILL/reopen cycles without affecting the production unit.

## Completed import staging retirement

Concurrent disk qualification exposed completed staging journals occupying the
16-upload ceiling throughout their recovery lease. Signed fix `7f01e3e` uses the
existing publication transfer and independent retry receipt to retire their
private staging immediately. Finite shares keep large completed files from
monopolizing small journal cleanup. The 16-journal and 128-unit limits, unfinished
upload leases, consumption authority, evidence ownership, retention and retry
windows are unchanged. No schema or storage migration is needed.

Full check and the normal hook passed 141 storage scenarios, 51 RPC scenarios,
17 SDK tests, remaining workspace checks and 215 ownership rules with no distinct
exceptions. Logs are `/tmp/sparrow-staging-retirement-check.log` and
`/tmp/sparrow-staging-retirement-commit.log`. The explicit daemon rebuild after
the hook is `/tmp/sparrow-staging-retirement-bin.log`. Installed image
`/home/paudley/.local/libexec/sparrow/sparrowd-schema42-7f01e3e` matches the selected
artifact, SHA-256 `a87dbe752a1b9b07fbc0370a5a00c6a8e794e82c7d971f6a01f4e44a989f4f68`.
Its sole capability remains `cap_net_bind_service=ep`. The stable daemon alias
selects it; the schema 42 CLI remains the qualified `b17f121` image.

Live evidence is `/tmp/sparrow-staging-retirement-live.lul9cbw_`: images/hashes,
unit/status, closed-store snapshot, configured invocation, listener and exact
saved handoff retries. The unit gracefully stopped PID 2062025, confirmed its
socket absent, captured the closed store and adopted PID 2222752. Schema 42,
format 37, store identity, 212 records, existing watch checkpoints and owned
replacement bytes are preserved. The listener remains `0.0.0.0:518`; TCP/Unix,
CLI/MCP/SDK handoff retries preserve their original receipts and bytes. No intake
or producer file was changed. The new closed snapshot has not separately been
restored.

The [concurrent disk qualification](performance-baseline.md#concurrent-mixed-format-workload-and-staging-retirement)
now completes 100 scoped query traversals and 20 mixed durable imports with
stable retries and restart persistence. It records 134 transport refusals and
bounded reconnects rather than hiding them. Matching-binary disk SIGKILL checks
also pass. These qualify the identified staging lifecycle fix; broad capture,
full SPARQL, forced reclamation and complete performance acceptance remain unmet.

## Frozen artifact match groups

Signed source `316d2c8` adds optional artifact grouping to the existing captured
record operation. Typed RPC, CLI and MCP share the same matcher and page renderer.
Groups reference unchanged page rows and carry frozen counts across the capture;
native text order, ranking, scores and witnesses are preserved. The
[interface contract](interfaces.md#artifact-match-grouping) defines count scope,
pagination and limits. No storage migration, runtime dependency, detector
relaxation or ownership exception was added.

Full check and the normal signed commit hook passed 142 storage scenarios,
51 RPC scenarios, 17 SDK tests, remaining workspace tests, release Clippy with
warnings denied and 217 helper ownership jobs with zero distinct exceptions.
Logs are `/tmp/sparrow-artifact-groups-check.log` and
`/tmp/sparrow-artifact-groups-commit.log`. Tests prove frozen counts after a new
artifact arrives, unchanged native rows/scores/evidence, selector conflicts,
all existing cursor-only page families, release and strict flag decoding.

All matching binaries and SDK examples were explicitly rebuilt after the hook
(`/tmp/sparrow-artifact-groups-bins.log`). Installed daemon
`~/.local/libexec/sparrow/sparrowd-schema43-316d2c8` has SHA-256
`d264be843de31744abcfc4b939f6ffc5d33644d54ac1abaed527fab64a76ebc4`
and sole capability `cap_net_bind_service=ep`. Installed CLI
`~/.local/libexec/sparrow/sparrow-schema43-316d2c8` has SHA-256
`a07d9fd53b4b834cb9f98788029919e229dfac29b59d7d90c16cd578b80d4156`.
Stable aliases select those immutable images. Matching events, bytes and handoff
example paths and hashes are recorded in the live evidence directory.

Adoption evidence is `/tmp/sparrow-artifact-groups-live.e3vtoo01`: image hashes,
unit/status, closed-store snapshot, saved handoff intents and retry verification,
`group-report.json` and `group-mcp.json`. The enabled unit gracefully stopped
PID 2222752 and adopted PID 2455426. The listener is still `0.0.0.0:518`,
reachable at `tcp://192.168.2.4:518`, with the same private Unix socket.
Store `store:46c88e949ccec658600b66c7becd5bb7`, format 37, 212 records, zero pins,
existing watch checkpoints and owned producer replacement bytes are preserved.
TCP/Unix/CLI/MCP/SDK saved handoff retries retain their receipts and exact bytes.
No producer file or intake changed. The new closed snapshot has not separately
been restored.

Live grouped source and text traversals compare unchanged native rows, scores
and ranking with ordinary captures. Every page group count agrees with an
independently counted ordinary capture, and indices partition page rows exactly.
MCP cursor-only continuation matches CLI, a changed grouping flag refuses
`CONFLICT`, explicit release invalidates continuation, empty grouped captures
return an empty list, and string/number/null flags refuse `INVALID_ARGUMENT`.
The schema 42 client refuses discovery. Final read handles and bytes are zero,
with unchanged record/pin counts and healthy owned watches.

Separate Btrfs corpus evidence is
`~/.local/state/sparrow/artifact-groups-qualification-62ll3f78`, containing the
driver, isolated configuration, daemon log and report. It cloned the closed
7,058-record corpus without opening its original store or configuring intake.
Source grouping captures 286 records across two pages and two artifacts with
143 matches each. The half-open first-pass receipt interval captures 3,529 records
across 24 pages with artifact counts 143, 2, 3,380 and 4. Scoped native ranked text
captures 12 rows with two artifact counts of six. Ordinary and grouped complete
traversals preserve exact row order, scores and witnesses; frozen counts and
page-local indices hold throughout. Every lease is explicitly released, final
handles/bytes are zero, records remain 7,058, and the isolated owner closes cleanly.
The broad grouped capture refuses `RESOURCE_LIMIT` with empty stdout under the
existing 2 MiB capture ceiling. One earlier collector attempt in
`artifact-groups-qualification-wxbc3hob` wrongly expected the refusal to print
the numeric ceiling; its evidence is retained as an unsuccessful collector run.
The corrected check matches the actual typed refusal code and message.

This is query-behavior qualification on a small copied corpus, not sustained
performance or full architecture acceptance. Full SPARQL, forced physical
reclamation, broad capture strategy and complete performance acceptance remain
unfinished.

## Independent RPC connection admission

Signed source `608327f` separates finite transport admission from simultaneous
storage work. Previously both semaphores used `max_concurrent_requests`, so four
idle capabilities could occupy all four connection slots. The daemon now uses
`max_rpc_connections` for shared Unix/TCP stream leases (1..256, default 16);
storage operations and intake retain their independent 1..64 budget.
The 60-second transport lease and typed operation `RESOURCE_LIMIT` remain.
Excess connections still disconnect before capability admission. No schema,
store format, runtime dependency or scoring law changed.

Full check and normal signed hook passed 142 storage scenarios, 52 RPC scenarios,
17 SDK tests, remaining workspace checks and 218 helper ownership rules without
distinct exceptions. Logs are `/tmp/sparrow-connection-budget-focused.log`,
`/tmp/sparrow-connection-budget-check.log` and
`/tmp/sparrow-connection-budget-commit.log`. Real-daemon acceptance holds five idle
typed peers with one storage permit, verifies a sixth can work, refuses a seventh
over either transport under the shared six-stream budget, and verifies that
disconnect returns its permit for successful ingestion. It also verifies the
default and actionable refusal of zero and oversized configured ceilings.

All matching bins/examples were explicitly rebuilt after the hook
(`/tmp/sparrow-connection-budget-bins.log`). Installed daemon
`~/.local/libexec/sparrow/sparrowd-schema43-608327f`, SHA-256
`88373769f440a155ed861d992b3a1fc879ee3e3d3f33524ee5e142247c7767d6`,
has sole capability `cap_net_bind_service=ep`. Stable aliases select the new
immutable images; matching CLI and SDK example bytes are identical to their
prior schema-43 images. Image paths/hashes and live qualification are in
`/tmp/sparrow-connection-budget-live.ekl8f0x_`. The enabled unit gracefully stopped
PID 2455426, captured a closed-store backup and adopted PID 2551939. Startup
reports 16 connections and four storage operations. The listener remains
`0.0.0.0:518`, with the same private Unix socket, store identity, format 37,
212 records, zero pins and healthy owned watches. No intake or producer file
changed; the new closed backup has not separately been restored.

`idle-report.json` proves six live TCP/Unix MCP clients remain usable while a
seventh CLI client reads status, with the four-operation budget unchanged.
Saved public handoff retries and schema-43 grouping regressions pass across
CLI/MCP/SDK and both transports, preserving receipts, native scores, bytes,
checkpoint clocks and the producer replacement. Final read handles/bytes are
zero. The separate
[concurrent repeat](performance-baseline.md#independent-connection-budget-repeat)
records zero transport disconnects, explicit storage concurrency refusals and
successful bounded stable retries. Full architecture acceptance remains unmet.

## Offline 30-day turnover qualification

Signed qualifier `c5e776c` completes 60 simulated daily cohorts on three new
Btrfs stores, covering no pins, a sparse record pin, and dense artifact pins.
[The report](turnover-qualification.md#qualified-run) binds source, installed
image, frozen inputs, independent count/native RDF comparisons, reopen boundaries,
release/refused resurrection, logical cleanup, RSS and measured physical lag.
This adds offline operational evidence; the enabled public server remains
source `608327f` at PID 2551939, schema 43 and format 37, without a restart or
production intake change. Full performance acceptance and forced physical
reclamation remain unfinished.
