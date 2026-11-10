# #158 delivery review 5: listing builds and constant-time status

## Scope

Commit 8ba701d on `paudley/158-serve-every-query-and-status-call`, diffed against ea1a4b2.
This review covers rulings (1) to (4) from the caller, which follow the plan's
"User rulings" and "Latency rework" sections. It also covers thread lifetime, concurrent
polls, lease rebasing, progress units, and the documentation and MCP texts. The untracked
`crates/sparrow-core/tests/measure_158.rs` was ignored.

I ran no cargo, make or build command, and I edited no source. I used `git diff`, file reads,
`grep`, log reads and `prose-check`.

## Evidence

- Commit hook `make check`: `/home/paudley/Active/sparrow/.stage/158/158-commit2.log`, EXIT=0.
  It ran 238 storage tests, 87 RPC tests, the census self-tests and `--cross --check`. The
  earlier run, `158-commit1.log`, was EXIT=1 on the census; that was fixed.
- The two listing-build tests: `158-focused3.log`, EXIT=0 (2 passed, 236 filtered out).
  `158-focused1.log` and `158-focused2.log` are earlier EXIT=101 runs whose causes were fixed.
- Clippy (`158-clippy4.log`) and fmt (`158-fmt1.log`): both EXIT=0.
- Latency probe `158-measure-300.log`: still running, with no result. There is no live-size
  run.
- `prose-check` exited 0 on the "Listing builds" part of `docs/interfaces.md` and on
  `.stage/158/pr-body-wip.md`.

Rulings confirmed by reading the code:

- (1) `status_counted` (`storage.rs` ~2683-2790) reads fixed keys plus the bounded `pin:`
  scan. It uses `counts: pending` while seeding. `exact_values` and `Positions::follows` are
  gone from the tree. Handle counts come from the ledgers (`catalog_lease.rs` `read_stats`,
  `Reads::mirror`), and status never locks a read registry or a build registry. Nothing in the
  status path grows with the store.
- (3) No path holds a read registry or the writer while it waits for a build:
  - `source_build_poll` and `artifact_build_poll` lock their build registry only to look up
    the build.
  - `artifact_read_cancellable` and `result_next` drop `reads` before `artifact_window`.
  - The `sources` build takes the writer inside its own thread.
- Progress units: `owed_entries` = (Artifacts + Records) - SourceSummaryDone. The pass calls
  `entry()` once per `artifact:` and `record:` entry past the checkpoint, which is the same
  quantity as status `total - done`. The source test asserts that equality.
- (4) Wire: the capnp `build @10`, the `catalog_receipt` encoder (set only when `Some`, with
  `total` 0 and `totalPending` while seeding) and the `agent/catalog.rs` renderer are
  consistent.
- Lease rebasing: on delivery, the `sources` capture gets `expires_at = now + LEASE_MS` and a
  fresh `started`. Artifact pages lease from the poll's `now`.

## Findings

### 1. REQUIRED: a finished build is consumed before its page is delivered, so a failed or retried delivery restarts the build

`listing_build.rs:153` (`Waiter::wait`) moves the result out and leaves `State::Delivered`.
The callers then retire the build before the page exists:

- `source_read.rs:454` removes the build, then calls `publish_sources` (`:461`).
- `artifact_read.rs:680` removes the build, then calls `artifact_pending_page`.

Anything that fails after that point discards the finished build:

- `artifact_pending_page` (`artifact_read.rs:692`, `:728`) takes the artifact registry and then
  the writer with no time bound. A stalled writer, such as the live 61 s stall in the plan,
  keeps the call blocked past the 30 s MCP deadline.
- The RPC's `CancelQuery` then cancels the call. The `cancellation.checkpoint()` after the
  writer lock, or the one in `read_work` (`rpc.rs:392`), fails. An undelivered `QueryReply`
  also releases the new handle (`rpc.rs:146`).
- `publish` can also refuse with `RESOURCE_LIMIT` at 8 artifact or source handles.

The client's retry of the same build cursor then gets `RESULT_EXPIRED` from `Builds::poll`.
A second concurrent poll gets `CONFLICT` (`listing_build.rs:156-159`), or a misleading
`RESULT_EXPIRED` "lease ended" (`source_read.rs:435`, `artifact_read.rs:650`). The only way
forward is a fresh listing, or the old continuation cursor, and both start a new full pass
over the unmigrated store.

This breaks the condition "following the cursor resumes the same capture and never restarts
the build", and it breaks it under the stalled-writer condition the rulings target. Fix: keep
the result and the build registration until a page built from it is published. For example,
make the result shareable (`Arc`) or restore it on failure, and remove the build only after
`publish` succeeds. A poll that arrives after delivery should then get a defined answer, not
`RESULT_EXPIRED`.

### 2. REQUIRED: a panicking build thread becomes an endless "still building" answer

The spawned closure (`listing_build.rs:250-255`) sets `State::Done` only when `work`
returns. The workspace profile does not set `panic = "abort"`, so a panic unwinds out of the
thread:

- The state stays `Running` and `read` stops advancing.
- Every poll (`listing_build.rs:283-286`) renews the lease, so a client that follows the
  cursor gets rows-free pages with frozen progress indefinitely. The build also holds one of
  the 8 build slots.

Before this change, the same panic surfaced as an RPC failure from the blocking worker.
Now it is a swallowed failure that looks like progress. Fix: add a drop or unwind guard in
the thread (or `catch_unwind`) that stores `Done(Err(STORAGE_FAILURE ...))` and notifies the
waiters.

### 3. REQUIRED: no test protects the `read` half of the progress condition

The ruling requires a test for "a page with no rows carries ... the build's progress (entries
read of the owed total)".

- Both tests (`tests/storage.rs:19790-19792`, `:19913`) check `total` only. `read` is checked
  only as `<= total`.
- The pause hook stops the build before the owed pass, so `read` is always 0 while it is
  observed.
- If the `worker.entry()` wiring (`source_read.rs` owed-pass closure,
  `artifact_read.rs:482`) were removed, both tests would still pass.

Fix: add a cheap check, for example a test hook that reports a build's progress after its
thread finishes, and assert `read == total`.

### 4. REQUIRED (acknowledged as open in the WIP PR body): no latency report, live-size measurement or current validation record

- Issue acceptance requires latency during against after. The coordinator ruling requires
  this at the live size (2.67M records), 20 samples per kind, at both sizes, plus the
  source-summary duration at the live size.
- `158-measure-300.log` has no result yet.
- `validation.md` still describes the pre-rework tree. It says "Nothing is committed", lists
  R5b `exact_values` (now reverted) and has no listing-build evidence.
- The stage `pr-body.md` is stale and must not be published. It contains the `LATENCY_TABLE`,
  `EVIDENCE_TABLE` and `REVIEW4_VERDICT` placeholders. It also claims that status "scans the
  unseeded keys" and may take "about 20 s per call", which contradicts ruling (1) and the code.

### 5. OPTIONAL: an abandoned artifact build has no test

The source test covers expiry, purge and thread end through the shared `Builds`. The
artifact-specific purge (`storage.rs` `purge_read_leases`, the `artifact_builds` line) and
window-crossing builds have no abandonment case. The artifact test also sets a 100 ms wait,
so it does not check page latency at the production budget.

### 6. OPTIONAL: a build thread's `Store` clone keeps the registry alive, so dropping the store never cancels it

The closures capture `self.clone()` (`source_read.rs:360`, `artifact_read.rs:591`). That
clone keeps `Unlocked`, its `Builds` and their `Build` alive, so `Build::drop` never cancels
when the daemon drops its store. The thread runs until it finishes or until its 60 s lease
runs out at a 4,096-entry check, and it holds the RocksDB handle and its file lock during
that time. This is harmless at process exit, but an in-process reopen of the same directory
would fail. A `Weak` or an explicit cancel-all on shutdown would close it.

### 7. OPTIONAL: build-held memory is outside the admissions and status

While a build runs, its `Completion` (the index set, up to one window, plus the deltas) is
held outside any admission. A finished result that nobody has taken stays held too, until a
purge. That can be up to 8 owed windows plus 8 source captures. Neither is counted in
`HANDLE_BYTES` or in status. `docs/interfaces.md:822` ("What it holds is charged to the
admission of each page that holds it") and `docs/architecture.md` ("what they hold is
charged as memory") do not describe this interval.

### 8. OPTIONAL: doc precision

- `docs/interfaces.md:847` says that "a further fresh listing refuses with
  `RESOURCE_LIMIT`" at 8 builds. A live artifact continuation that crosses a window also
  starts a build, so it can refuse the same way.
- `docs/architecture.md` (the paragraph at ~556) says "the source listings add what the
  counter seeding still owes". That reads as `store-counters` seeding; the paragraph means
  the `source-summary` migration.
- A `sources` build page reports `total_items: 0` with rows `[]`. Consumers that read
  `total_items` without following `next_cursor` would see an empty catalog. A sentence in the
  `sources` contract, plus a note to the katamari and brainstem consumers, would close this.

VERDICT: BLOCKED
