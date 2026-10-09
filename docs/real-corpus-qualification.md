# Local sample investigation qualification

An isolated Sparrow instance imported immutable copies of the supplied sample
locations on 2026-10-08 local time (2026-10-09 UTC). Evidence is
`~/.local/state/sparrow/real-corpus-qualified-us8klaoz`. Sample bodies, original
identities, native receipts and query outputs remain there, outside tracked
source. This is bounded sample qualification, not a representative corpus census
or production-volume acceptance.

The preparer uses read-only, no-follow descriptors, compares device/inode/size/
nanosecond modification and change observations before and after each bounded
read, and freezes SHA-256-identified copies. Each full-file input is at most
2 MiB. The mentation input is the complete-line prefix within a 2 MiB read of a
historical rotated segment, explicitly identified as a byte-range sample rather
than a complete original file. Its exact retained prefix is 2,076,734 bytes.
No production watch or source deletion is configured. No Brainstem/model
lifecycle operation is involved.

| Sample window | Bytes | Native records | Captured key/kind rows | Qualified text matches |
| --- | --- | --- | --- | --- |
| Historical corrections JSONL | 31,174 | 30 | 29 | 27 |
| Historical discards JSONL | 9,871 | 12 | 24 | 12 |
| Rotated mentation JSONL prefix | 2,076,734 | 72 | 4,848 | 1 |
| Complete observed pacman log | 1,132,414 | 13,720 | Not applicable | 7 error matches |
| Brainstem terminal manifest JSON | 2,539 | 1 | 413 | 1 |
| First selected terminal `.bin`, declared JSON | 47 | 1 | 3 | No string-value case |
| Second selected terminal `.bin`, declared JSON | 1,685 | 1 | 352 | 1 |
| Third selected terminal `.bin`, declared JSON | 4,091 | 1 | 904 | 1 |

These particular binary artifacts were admitted with an explicit JSON hint and
discovered through native projection. Their results do not classify other `.bin`
files. Seven full files and one declared prefix total 3,258,555 bytes and 13,838
records. The initial eight staged typed imports took an observed 31.091 seconds,
including client startup and durable acceptance, without a throughput claim.

## Verified investigations

The daemon and CLI initially used frozen source `2f1f8ff`, schema 48, with only
a private Unix socket and a new store. Exact artifact downloads reconstruct every
saved input through bounded reads and match its independent saved digest.
Independent newline/final-record counts agree with receipts for JSONL/log inputs;
each JSON document produces one record. Record membership checks, native key and
value discovery, native text ranking, context and one-record pin/release use the
production typed operations. Cursor-only CLI and MCP continuations return exactly
the same page and preserve the original receipt. Text cases compare CLI/MCP rows
and native ranking identities. The numerical-only first binary sample has no
string-value text case, rather than an invented search term.

Corrections are imported again under a separate, explicitly declared
`/timestamp` RFC 3339 event-time profile. This is a qualification interpretation,
not an inferred production subscription. All 30 records match their historical
UTC day, independently of their October acceptance time. An interval ending at
the earliest observed timestamp returns zero, proving the exclusive upper
boundary. The original unprofiled source returns zero for that event interval
and explicitly reports 30 unknown times with complete membership. The combined
component field, text and event-time query returns 27 records and one artifact
group; CLI and MCP answers agree. No acceptance-time fallback is used.

After this additional artifact, the store has 13,868 records. Pins and result
leases return to zero. The private daemon's SIGINT teardown returns exit code
zero. Reopening under frozen updated source `e28431f` preserves store identity,
membership and every original sample's exact bytes. It again tears down with
SIGINT and exit code zero. Configuration, images/digests, source references,
preparation/investigation programs, native outputs, MCP transcript, initial
failures and successful scoped cases remain in the evidence directory.

## Refusals and unfinished acceptance

The initial mentation key request reaches the default 10,000 native JSON
occurrence visits and returns `RESOURCE_LIMIT` without a page. An explicit
`scan_budget=100000` request captures all 4,848 key/kind rows for paging. The
updated implementation reports `json_occurrence_budget=10000`, with the
admitted option to raise the request or narrow its source/time/evidence scope.
Record-visit exhaustion is distinguished and includes its ceiling. Neither
default nor maximum admission was raised by this change.

The broad 13,720-record pacman capture exceeds the 10,000-match ceiling and
refuses. Explicit selections of at most 1,000 known receipt IDs verify all its
records, and an error-text search returns seven matches. These are separate
bounded captures, not a successful broad query or one shared snapshot across
the whole file. The broad refusal remains recorded. Plain-log historical time
extraction is not qualified by the JSON timestamp-profile case; the pacman
evidence remains acceptance-time scoped.

Unrestricted source-local context for the singleton Brainstem manifest exhausts
aggregate catalog decoding while traversing unrelated earlier records. An
explicit one-millisecond acceptance interval around that artifact succeeds.
This qualifies that interval-scoped context only. Efficient source-local context
in a large mixed-source catalog remains unfinished; increasing the visit budget
does not increase the independent catalog-byte allowance.

The initial daemon's kernel-reported `VmHWM` was 272,408 KiB. Its pre-shutdown
process I/O counter reports 3,129,151,488 written bytes across ingestion, native
storage maintenance and result staging. This does not isolate engine write
amplification or establish a worst-case memory bound. Final closed files after
reopening total 51,435,826 bytes (51,462,144 allocated): 50,578,481 SST bytes,
448,925 blob bytes and 408,420 control bytes, with no WAL bytes. The relative
costs of parsing, RDF/text indexes, WAL/compaction and captured results still
need separate measurements. These observations do not qualify sustained
10 GiB/day, quota pressure, forced reclamation, power loss or reboot behavior.

The public service received no sample ingestion. It was independently updated
to the verified budget-diagnostic build; its store remains at 212 records.
See [the development runtime](development-runtime.md#actionable-json-capture-budgets)
for the source gate, installed image and public readback evidence. Complete
governed SPARQL and correlation remain required integration work.
