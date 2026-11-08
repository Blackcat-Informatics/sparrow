# Review 3: #146, recheck of review 2 and the D118 retire command

## Scope

This round covers the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/146-keep-status-and-ingest-responsive-while`
against base `d912922`: the `git diff` plus the untracked files.

1. A recheck of review 2's N1, N2 and O7 to O10 against the current source, using the
   "Review 2 rework" section of `implementation.md`. R4 belongs to the orchestrator and stays
   recorded as open.
2. A full review of the D118 scope, as `plan.md` records it (D118 amended 05:20, and the
   orchestrator's brief):
   - core `Store::consumption_retire` and `consumption_retired`;
   - server `ClaimRoot::retire`;
   - the `watch_admin` and `watches.rs` worker path;
   - the `watchClaimRetire` wire fields;
   - the admin gating;
   - CLI and MCP;
   - the docs (`interfaces.md` "Retiring an unrecoverable claim", the `user-unit-upgrade.sh
     --help` bullet);
   - the two new tests.

## Evidence

- My own checks:
  - `bash -n` on `deploy/verify-backup.sh` returned 0.
  - `shellcheck -x` on `deploy/verify-backup.sh` and `deploy/user-unit-upgrade.sh` returned 0.
  - `deploy/libexec/sparrow-backup` parses as Python (`ast.parse`, exit 0).
  - I ran no cargo command. Everything else is a code trace, cited by file:line below.
- Checks the implementer reports with exit 0, which I did not rerun:
  - fmt;
  - release clippy `-D warnings` on four crates;
  - core: lib 37, storage 232;
  - server: lib 28, sparrowd 8, rpc 86;
  - client: 22 + 1;
  - census-check: 320 jobs;
  - shellcheck;
  - py_compile.
- The tests these findings rely on exist where cited:
  - `status_refuses_once_the_writer_is_poisoned` (`tests/storage.rs:19753`);
  - `verify_backup_reports_pending_counts_as_not_verifiable` (`tests/rpc.rs:4852`);
  - `retire_claim_clears_only_an_unrecoverable_claim_and_lets_the_drain_reach_zero`
    (`tests/rpc.rs:4098`).

## Recheck of review 2

- **N1: closed.**
  - `Store::unique_payload_chunks` returns `None` while the counters seed
    (`storage.rs:2510-2520`), and the closing line logs `unique_payload_chunks=pending`
    (`shutdown.rs:58-67`).
  - `sparrow-backup`'s regex accepts `pending`. `recorded_count` writes
    `live_records=pending` and refuses any other `counts` value before the stop, the same way
    it refuses other bad states.
  - `verify-backup.sh` never compares a pending count. It expects `pending` in the closing line
    of a pending copy, and `MEASURE` prints `counts=`.
  - `user-unit-upgrade.sh:258` prints `counts=` beside `live_records`.
  - `RECOVERY.md` and `deployment.md` describe all of this.
  - See O11 for the one remaining wording conflict.
- **N2: closed.** `refuse_poisoned` checks `Mutex::is_poisoned`, which never waits. These call
  it: `status_counted`, `unique_payload_chunks`, `durability_status`, `pressure_status`,
  `recompression_status` and the new `consumption_retired`. A test poisons the writer and
  asserts `STORAGE_FAILURE` with the same message the locked paths give.
- **O7: closed** (`interfaces.md:686-688`).
- **O8: closed** (`migration.rs:291`, `:335`).
- **O9: closed.**
  - `Job::Migrate { defer }` waits inline for the first page after a retry tick, and for a page
    whose background deferral finished while another duty ran
    (`maintenance.rs:72`, `:106-110`, `:214-216`, `:258-266`).
  - The inline wait blocks the worker for at most the deferral bound, and only in those two
    cases.
- **O10: closed.** The example switches from `sparrowd-schema68-2aa56a9`, and the text names
  the 69-client refusal.

## D118 review

What holds:

- **Refusals.** `consumption_retire` (`consumption.rs:783-848`) refuses each of these under the
  writer lock:
  - a caller or reason that is blank, more than one line, or longer than 512 bytes;
  - an unknown journal;
  - a removal-confirmed journal;
  - a journal on another watch;
  - a published journal (`published_id`);
  - a journal never claimed;
  - a journal already retired;
  - a 65th retirement.
- **Durable record.** One batch puts `consumption-retired:ID` (the journal, `retired_at`,
  caller and reason) and deletes `consumption:ID`. The record goes through `Writer::apply`, so
  it is durable, and the claim leaves `consumption_pending` and the 16-journal bound.
- **Prefixes do not collide.** `consumption-retired:` never matches the `consumption:` prefix
  scans.
- **Source must be missing.** `ClaimRoot::retire` (`consumption.rs:531-571`, server) checks the
  watch scope and the private namespace. It refuses while `.sparrow-inflight/ID` opens and
  never touches a path.
- **Admin gating.**
  - The ingest socket and anonymous TCP refuse the call (`ingest_gate.rs:180`).
  - Query-only treats it as a write (`query_only.rs:133`).
  - The handler requires an identified administrator, either `unix uid= pid=` or
    `tcp-chap peer=`, and otherwise returns `PERMISSION_DENIED` (`rpc.rs:1836-1841`).
- **Worker path.** It runs on the watch worker that owns the private directory, drops the
  claim's retry entry, and logs one line: `sparrowd: watch=… claim=… retired-unrecoverable
  caller=… reason=… path=…`.
- **Listings.**
  - Status and `watch list` carry `retired_claims` with state `retired-unrecoverable`.
  - `ingest_get` reports `retired_unrecoverable`.
  - MCP `watch_claim_retire` is marked not read-only and destructive.
- **Docs.** The docs and the `--help` bullet match the behavior.
- **Tests.**
  - The core test covers the refusals, pending going from 2 to 1, reopen, and a second
    retirement being refused.
  - The RPC test covers CLI and MCP refusal of a claim whose private source exists
    (`CONFLICT`, "still exists"), retirement through the CLI with a `unix uid=` caller, both
    listings, pending reaching 0, the failure clearing, and exactly two journal lines.

## Findings

### D1 REQUIRED: retiring a claim whose consume import already began leaves an import journal that can never be removed

After the claim, the consume worker begins the claim's staged import under the same identity:
`import:ID`, `ImportManifest.consumption = true`.
`publish_consumption` and `consumption(&manifest.id)` link the two (`consumption.rs:583-600`).

Such a manifest has no way out except publication:

- `import_reclaim` skips every consumption manifest without a publication:
  `(manifest.consumption && manifest.published_artifact.is_none())` (`import.rs:994-997`).
- A consumption manifest never expires: `(!self.consumption && now >= self.expires_at)`
  (`import.rs:250`).
- `import_cancel` refuses it: "consumption staging owns a claimed source; reconcile its journal
  instead" (`import.rs:825-829`).
- After retirement, publication is impossible as well, because `require_consumption` no longer
  finds the journal.

`consumption_retire` deletes only `consumption:ID`. So any retired claim whose import had begun
leaves its manifest, and its staged chunks, in place permanently:

- Each such claim occupies one of the 16 import journals (`MAX_IMPORTS`, `import.rs:16`,
  `:1141-1146`).
- Its staged bytes stay on disk.
- The store allows 64 retirements, so 16 of these block every staged import. That includes
  every consume publication and every inline ingest large enough to stream.

The live claim this command exists for (`import:252d1ffe…`) was cut off during the incident in
which consume publications missed their deadline, so an import already in progress is the
likely shape of real cases. The test never covers it: its claims stop at `ClaimRoot::recover`
(`Recovery::Claimed`, `consumption.rs:423-524`), before any import begins.

Required:

- Retirement disposes of the claim's consumption import in the same batch as the retirement,
  using the reclaim path's chunk and preparation removal. Alternatively, it marks the manifest
  reclaimable and `import_reclaim` then removes it.
- Add a test with a claim whose import has begun: before retirement the import journal is
  occupied, and after retirement and one reclaim pass the slot and the staged chunks are free.
- Update `docs/interfaces.md` "Retiring an unrecoverable claim" to match.

### R4 carried (orchestrator)

The PR says "Refs #146" and does not close the issue while done-when 3 is owed. This stays
recorded and open.

### O11 OPTIONAL: `verify-backup.sh` exits 0 with the payload-chunks check unverified

The usage text still says "A backup counts as verified only once this exits 0"
(`verify-backup.sh:33-34`). A pending copy, or a pending SOURCE, now ends "verified except
payload-chunks: not verifiable (counts pending)" with exit 0. A backup whose SOURCE has no
count still exits 1 as NOT VERIFIED.

The behavior is documented in `RECOVERY.md`. Still, a machine caller cannot tell from the exit
status which kind of verification it got. Either use a distinct nonzero exit, following the
unrecorded case, or qualify the usage sentence.

### O12 OPTIONAL: retirement clears the watch failure even when another claim caused it

`retire_claim` sets `detector.failure = None` whatever the failure was (`watches.rs:851-852`).
With two stuck claims, retiring one hides the other's failure until its next retry sets it
again. Clear the failure only when it came from the retired claim's recovery.

### O13 OPTIONAL: the drain assertion and the gate refusals are not tested directly

- The RPC test's check that no file waits is vacuous, because no file was waiting during the
  stuck state. A file added while the claims were stuck, and consumed after retirement, would
  show the drain itself.
- No test sends `watchClaimRetire` over anonymous TCP or the ingest socket. The refusal comes
  from the gate's method list, so it is structural rather than exercised.

VERDICT: BLOCKED
