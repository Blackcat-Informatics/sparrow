Reviewer model: claude-opus-5-5 (opus)
VERDICT: BLOCKED

# #158 delivery review 6: listing builds after the review-5 fixes

## Scope

Head 0f2857c on `paudley/158-serve-every-query-and-status-call` (PR #169, WIP), diffed against
origin/main f1219ee (`git diff f1219ee...HEAD`). The fix commits b3162ae and 0f2857c were read as
patches. The merge f5788dd was not reviewed as feature work. Rulings were taken from `issue.md`
and from `plan.md` `## User rulings`, including the coordinator's plan acceptance at `plan.md:60`.

This was a static review. I ran no cargo, make or build command, edited no source and wrote
nothing to /tmp. I used `git show`, `git diff`, `git grep`, file reads, the logs under
`/home/paudley/Active/sparrow/.stage/158/`, and `prose-check`.

## Evidence

- Commit hook `make check` at the head: `158-commit6.log` EXIT=0, with 241 storage tests and
  91 RPC tests. At b3162ae: `158-commit4.log` EXIT=0. `158-commit5.log` EXIT=1 (fmt-check),
  which was superseded.
- Listing-build tests: `158-focused5.log` EXIT=0, 2 passed, 239 filtered.
- Clippy on the merged tree: `158-clippy7.log` EXIT=0. RPC target: `158-rpc4.log` EXIT=0,
  91 passed.
- Sync: `158-sync2.log` EXIT=0. The PR body and `validation.md` both say that the merge
  commit's own hook output is not logged. That is correct.
- Probe: `158-measure-300.log` EXIT=0 and `158-measure-2670.log` EXIT=0 (15321.65 s).
  `158-probe-build2.log` (20:49) postdates the sync (20:15), so the probe ran on the merged
  tree with b3162ae. 0f2857c changes only a test.
- I checked every cell of the latency table in `pr-body-final.md:23-32` and
  `validation.md:49-58` against the two logs, and all of them match. The TO_ROWS table
  (`pr-body-final.md:45-48`) and the migration totals (6029 pages, 824.5 s; 682 pages, 39.4 s)
  also match. The claim that median context_rare (26.5 s) and records (27.7 s) meet 30 s while
  their p95 (41.9 s, 48.3 s) and max do not matches `158-measure-2670.log:9,12`.
- `prose-check` exited 0 on `pr-body-final.md`, `validation.md` and `docs/architecture.md`. On
  `docs/interfaces.md` it flagged only pre-existing `UInt32`/`UInt64` lines, none of them in
  this branch's hunks.
- Attribution: no agent credits or branding appear in the changed files or in the commit
  messages f1219ee..HEAD.

Review-5 findings, checked:

- R1 (core level): fixed. `source_read.rs:453-469` and `artifact_read.rs:672-690` keep the
  finished result (`State::Done`, `listing_build.rs:48-53`, cloned by `Waiter::wait` at
  `:159-187`). They retire the build only after `publish_sources` or `artifact_pending_page`
  succeeds. A failed build is retired and its error is surfaced. The registry lock is not held
  while a call waits or takes the writer (`artifact_read.rs:685`). A retry renews the lease
  (`listing_build.rs:302-315`). A narrow window remains at the RPC layer (O1). The test that
  claims to cover this fix does not exercise it (finding 1).
- R2: fixed. `catch_unwind` (`listing_build.rs:271-281`) turns a panic into
  `Done(Err(STORAGE_FAILURE))` and notifies the waiters. The `Running` guard (`:197-203`)
  decrements the running count on every exit, including a spawn failure.
- R3: fixed for sources. The hold is now at entry 1 (`storage.rs:19848`), and the test asserts
  `read == 1` (`storage.rs:19937-19940`), so removing the `worker.entry()` wiring fails it. The
  artifact test depends on the same hold firing to get a rows-free first page.
- R4: the measurements, validation and PR body now exist. Findings 2 to 5 cover what remains.

Status, ruling (b): confirmed constant-time. `status_counted` (`storage.rs:2767-2874`) reads
one snapshot: fixed keys, `migration::report`, the bounded `pin:` scan and
`linked_physical_bytes`, which lists files and does not scan keys. It reports `counts: pending`
until `positions.seeded()`. Handle counts come from `read_lease_stats` ledgers. No build or read
registry and no writer is locked. `exact_values` and `Positions::follows` are absent from the
tree. Measured: 0.78 ms p50 during seeding and 1.01 ms with the writer held.

Other areas checked with no finding: the wire (`sparrow.capnp` `build @10`, `CatalogBuild`), the
`rpc.rs:37-42` encoder, the `agent/catalog.rs` renderer (`counting` while pending), the MCP
texts, the expiry purge of both build registries (`storage.rs:2702-2713`, called from
`expiry.rs:88`), `resultRelease` of a build id (`result_read.rs:73-77`), the 8-build cap
(`listing_build.rs:244`) and the `helpers-ledger.toml` `listing-build` job. The stale `ready`
entry points were removed. No `INDEX_NOT_READY` remains under `crates/`. The remaining
mentions in `docs/development-runtime.md` and `docs/performance-baseline.md:47` are recorded
historical runs, not read contracts.

## Findings

### 1. REQUIRED: the retry-after-failed-delivery test (0f2857c) never reaches the build, so it does not protect the fix

`crates/sparrow-core/tests/storage.rs:19969-19979` passes a token that is already cancelled to
`source_read_with_syslog_cancellable`. That function calls `cancellation.checkpoint()?` at
`crates/sparrow-core/src/storage/source_read.rs:325`, before it parses the cursor (`:326`). The
call therefore returns `CANCELLED` without polling, waiting on or publishing the build. The
assertion `(1, 0, 1)` "the finished build waits for its page" holds for any implementation,
including the pre-fix code that removed the build before publishing. Every later step of the
test also passes on that code.

The coordinator's contract asks for a test of "retry after a failed delivery resumes". No test
covers it. `validation.md:16`, `pr-body-final.md:84` and the 0f2857c commit message say that
one does.

Fix: make the call fail after `waiter.wait` returns the finished result and before the page
exists. For example:

- add a dev-only hook that cancels between the wait and `publish_sources` /
  `artifact_pending_page`, or
- make `publish` refuse. A build started with a small `page_size`, so that the page has a
  `next_cursor`, plus eight source handles already held gives `RESOURCE_LIMIT` at
  `catalog_lease.rs:446`.

Then assert that the retry on the same cursor delivers the rows with builds-ever-started still
1. Covering the artifact path the same way is the stronger version.

### 2. REQUIRED: pages do not return within the 5-second budget the plan acceptance names, and the test accepts up to 6 s

The ruling at `plan.md:60` says: "every page returns within its 5 second budget". `BUILD_WAIT`
(`crates/sparrow-core/src/storage/listing_build.rs:31`) is 5,000 ms for the wait alone. Page
assembly comes on top of it, so every rows-free page exceeds 5 s:

| Measurement | Value | Log |
| --- | --- | --- |
| `sources_page` p50 / max | 5000.27 / 5098.85 ms | `158-measure-2670.log:10` |
| `artifacts_page` max | 5085.00 ms | `158-measure-2670.log:11` |
| Slowest TO_ROWS page | 6473.74 ms | `158-measure-2670.log:17` |

The production-budget test asserts `waited < 6 s` (`crates/sparrow-core/tests/storage.rs:19927`),
so it does not protect the stated condition. The PR body reports "within 5.1 s ... slowest
6.47 s" (`pr-body-final.md:35`) but does not say that the condition is missed.

Fix, either of:

- Reserve headroom so a page returns inside 5 s. For example, derive the wait from a page
  deadline and keep the call's own work after the wait bounded. Then assert `< 5 s`.
- Record a coordinator ruling verbatim that the 5 s bounds the wait, not the page, and state
  the measured overrun in the PR.

### 3. REQUIRED: the PR does not state how long source-summary runs at the live size

The ruling at `plan.md:60` says: "The PR states how long source-summary itself runs at the live
size". `pr-body-final.md:39` and `validation.md:60-61` say that the probe does not time it and
that its duration "is not separated". The only figure given is the combined 824.5 s for all
five migrations.

Fix: measure source-summary alone at 2.67M, either with a per-migration timing in the probe or
with the `--measure-migration` path that `docs/interfaces.md:894-896` names, and state it.

### 4. REQUIRED: the deferral of context and records to a separate issue has no recorded ruling, and the issue is not filed

The recorded ruling (`plan.md:60`) says: "records and context get the same treatment if over
30 s at live size". At 2.67M, context_rare reaches a p95 of 41.9 s and a max of 83.4 s, and
records reaches a p95 of 48.3 s and a max of 48.7 s (`158-measure-2670.log:9,12`). Both are over
30 s.

- `validation.md:68-70` defers the treatment citing a "coordinator ruling". No such ruling is
  recorded under `## User rulings`.
- `pr-body-final.md:37` says "It is filed as a separate REQUIRED issue (ISSUE_TAIL)", and
  `:71` repeats it. `ISSUE_TAIL` is an unfilled placeholder, so the claim that it is filed is
  false.

Fix, either of:

- Record the coordinator's deferral ruling verbatim, with its time, in `plan.md`; file the
  issue; and put its number in the PR body.
- Give context and records the listing-build treatment in this PR.

### 5. REQUIRED: PR body text that does not match the evidence

The PR body must be corrected before it is published:

- `pr-body-final.md:21`: "20 samples per kind". The "status, writer stalled" row (`:26`) is
  `n=5` (`158-measure-2670.log:20`, `158-measure-300.log:20`).
- `pr-body-final.md:84`: "Listing-build tests, with the retry-after-failed-delivery check".
  See finding 1.
- `pr-body-final.md:91`: the `REVIEW_LINE` placeholder. The `ISSUE_TAIL` placeholder is
  covered by finding 4.

### O1. OPTIONAL: a narrow window between building and delivering a page still loses a finished build

Core retires the build as soon as the page is built:

- `source_read.rs:467-468`
- `artifact_read.rs:688-689`

The RPC layer can still fail to deliver that page:

- `read_work` checks cancellation after core returns (`crates/sparrow-server/src/rpc.rs:410`).
- An undelivered `QueryReply` releases the new handle (`rpc.rs:143-149`).

If the client cancels in that window, a retry on the build cursor gets `RESULT_EXPIRED` and has
to start a fresh pass. `docs/interfaces.md:883-885` correctly limits the guarantee to a call
that fails "while its page waits for the writer", so the docs are accurate. The case is rare.

The delivered reply already releases the build id it supersedes (`rpc.rs:133,146`).
Retirement could therefore move there, with core leaving a delivered build to that release or
to lease expiry.

### O2. OPTIONAL: a starting call that fails while it waits leaves a build no client can reach

`source_read.rs:375` and `artifact_read.rs:597` return the wait error (for example
`CANCELLED`) without removing the build they just registered. The caller never learns its
cursor. The build holds one of the eight slots, and its thread runs until its lease ends
(at most 60 s).

For a live artifact window continuation the cost is higher. The old handle is not superseded,
so a retry on the old cursor reaches `Next::Window` (`artifact_read.rs:536-547`) and starts a
second build of the same window, which restarts that pass.

Fix: remove the build when the call that started it fails.

### O3. OPTIONAL: the abandoned-build test cannot tell cancellation from completion

`tests/storage.rs:19999-20015` purges at expiry and then waits for the thread to end, on a
store whose owed pass is a handful of entries. The thread ends whether or not `Build::drop`
cancels it, so removing the cancel would not fail the test. The ruling's point is that an
abandoned build releases its snapshot early.

Fix: assert that the abandoned build's result is `CANCELLED`, or hold it with enough owed
entries left that only cancellation can end it in time.

### O4. OPTIONAL: doc and ledger precision on build memory and deadlines

- `docs/interfaces.md:859-861`: "bounded by the window and capture limits" holds for an
  artifact window. A `sources` build's `Completion` deltas are checked against `SCAN_BYTES`
  only after the owed pass (`source_read.rs:538-565`).
- `docs/architecture.md:572-574`: "what they hold is charged as memory".
  `docs/architecture.md:626`: "so no page outlasts a caller's deadline". The page served from
  a finished artifact build takes the writer with no time bound (`artifact_read.rs:736`).
- The `helpers-ledger.toml` `source-summary-counters` summary says "charging what it holds to
  the page admission". Like the architecture text, it does not cover the interval before the
  page exists.

### O5. OPTIONAL: report completeness

- The 300k "after" figures were measured (`158-measure-300.log:23-29`) but are not in the PR
  table.
- "max (83.4 s)" (`pr-body-final.md:36`) is the context max only. The records max is 48.7 s.
- "roughly the slow tail of calls ... can time out" is vague. The log gives p95 above 30 s,
  not a count.
- `validation.md:18` lists O5 among the items closed by "docs updated". O5 was the missing
  abandoned-artifact-build test, and it is still open.
