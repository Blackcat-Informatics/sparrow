# Review 4: #146, recheck of review 3 (D1, O11, O12, O13)

## Scope

This round covers the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/146-keep-status-and-ingest-responsive-while`
against base `d912922`. Following the "Review 3 rework" section of `implementation.md`, I
rechecked:

- D1: `Writer::end_retired_consumption_import`, the retirement batch in `consumption_retire`,
  the `import_reclaim` keep rule, and the test at `tests/storage.rs:6154`;
- O11: the exit status of `verify-backup.sh`;
- O12: `Detector.failed_claim`;
- O13: the rebuilt test at `tests/rpc.rs:4117`, and the refusals at `rpc.rs:1617` and `:1888`.

I also checked behavior these fixes change. R4 belongs to the orchestrator and is still open.

## Evidence

- My own checks:
  - `bash -n` on `deploy/verify-backup.sh` returned 0, and `shellcheck -x` returned 0.
  - I ran no cargo command. The findings below come from tracing the code.
- `/tmp/s146-r3-server.log` shows one passing run: rpc 86/86, with
  `retire_claim_clears_only_an_unrecoverable_claim_and_lets_the_drain_reach_zero ... ok`.
  One pass does not settle (a); see T1.
- The implementer also reports fmt, clippy, core 37 + 233 and census-check, all exit 0. I did
  not rerun them.

## Recheck

### D1: closed

- `consumption_retire` stages the retirement record, the deletion of `consumption:ID`, and (when
  the claim's import began) the manifest with `expires_at = min(expires_at, now)`, all in one
  batch committed through `Writer::apply` (`import.rs:1052-1071`). A manifest at that ID that is
  not an unpublished consumption staging is refused with `STORAGE_FAILURE` rather than altered.
- `import_reclaim` now skips an unpublished consumption manifest only while its claim is not
  retired (`import.rs:996-1000`, `consumption::retired`). It frees the retired claim's import
  through the existing expired-lease path:
  - chunks, then layout pages, then the private preparation, then the manifest delete;
  - the manifest delete is what frees the import journal slot.
  There is no second removal path.

**(b) Promptness.** The maintenance worker runs the reclaim duty on a 1 s interval
(`maintenance.rs:173`). It is fairly selected and skipped only below the free-space floor. Each
pass removes up to 128 staging units:

- a small staged import is gone on the first pass after retirement;
- a large one is gone after its chunk count / 128 passes.

The backup drain checks only waiting files and pending claims, and retirement drops the claim
from pending in its own batch, so the drain never waits for reclamation. The 16-slot ceiling
frees as soon as the manifest is deleted. That is prompt enough for both.

**(b) Resurrection or publication before reclaim.** Neither can happen. I traced each path:

- `import_publish`, `publish_consumption` and `begin_import` all go through
  `require_consumption`. That reads `consumption:ID` or the completed key, never the retired
  record, so after retirement they refuse with `CONFLICT` ("consumption request has no recovery
  journal").
- No path renews a staging lease (`expires_at` is set only at begin, and only lowered here), so
  the expired lease stays expired.
- The worker drops the claim's retry entry, and `consumption_pending` no longer lists it, so no
  consume work restarts.
- Appends and seals on a consumption manifest skip the lease check (`require_live` ignores
  expiry for consumption), but nothing holds the claim to call them. Any bytes staged would go to
  the same reclaim.

### O11: closed

`verify-backup.sh` exits 3 when the payload-chunks check is not verifiable (`:574-577`).

- The usage text lists exits 0, 1, 2 and 3 and says exit 3 is not verified (`:33-37`).
- `RECOVERY.md` and `deployment.md` match.
- `verify_backup_reports_pending_counts_as_not_verifiable` asserts `Some(3)` for both runs.

### O12: closed (c)

Every path that sets the watch failure also sets `failed_claim`:

- claim recovery or publication failure: `failed_claim = Some(id)` (`watches.rs:1299-1301`), with
  the failure text `claim ID: …`;
- claim success: `failure = None`, `failed_claim = None` (`:1312-1313`);
- detector-level failures: `failed_claim = None` (`:405-406`, `:410-411`);
- worker-level failures: `failed_claim = None` (`:1142-1143`, `:1198-1199`);
- initialization: `None` (`:299`).

`retire_claim` clears the failure only when `failed_claim` names the retired claim
(`:860-862`). No other site writes `failure`. The old failure text survives as a substring
("unpublished claimed source is missing"), so the docs and the `--help` bullet still describe
it.

### O13: refusals closed; the drain is now shown; the rebuilt test is racy (T1)

- The anonymous-TCP CLI `watch retire-claim` is refused as unauthenticated (`rpc.rs:1609-1618`).
- The ingest socket's `watchClaimRetire` is refused (`rpc.rs:1887-1893`, in the shared
  refusal-code loop).
- The drain is no longer vacuous. A file written while the cause is still pending is consumed
  after the cause is retired, with `completed_claims >= 1`, `pending_claims == 0` and no waiting
  file.

## Findings

### T1 REQUIRED: the rebuilt retire test identifies the failure's cause with a race

The test assumes that a failure value holding for 300 ms is "the last word of its burst"
(`rpc.rs:4223-4237`). It then retires the other claim and immediately asserts that the failure
still names the cause (`:4245-4258`).

The two claims in one retry burst do not fail milliseconds apart:

- After the first claim fails, the worker drops its consume RPC connection
  (`watches.rs:1308-1310`: `client = None`).
- The second claim, due moments later, finds no connection. It may reconnect only once
  `connect_after`, which is 1 s after the previous connect, has passed (`:1226-1236`). Until then
  it is re-queued every 50 ms (`TICK`, `:1241-1245`).
- So the second claim fails about 1 s after the first, in every burst. The order is stable: the
  claims have the same backoff counts and are scanned in key order.

A 300 ms double read that falls inside that gap (two status CLI calls plus the sleep, about
0.4-0.6 s) reads the first claim's failure twice and names it as the cause. The test then
retires the real last-failing claim as "other". If that claim's own failure lands between the
reads and the retirement, `failed_claim` is that claim:

- the retirement clears the failure;
- `kept` is empty;
- `assert!(kept.contains(&cause))` fails.

Bursts recur at 5 s, then 10 s, then 20 s, and the loop starts at whatever point the earlier
CLI/MCP steps leave it. Under the host's load, CLI spawn time widens this window. A gate test
that passes or fails depending on timing will fail commits at random.

Required: decide the cause deterministically. Options:

- Make only one claim fail at the time it is read. For example, create the second
  unrecoverable claim only after the first claim's failure is established, or retire a pending
  claim whose retry is not yet due.
- Test the `failed_claim` rule where its inputs are controlled: `retire_claim` on a `Detector`
  whose `failed_claim` is set.
- At minimum, read stability over more than one full burst (the 1 s connect throttle plus
  margin) and retire within the same quiet interval. That narrows the race but does not remove
  it.

### R4 carried (orchestrator)

The PR says "Refs #146" and does not close the issue while done-when 3 is owed.

### O14 OPTIONAL: the core D1 test's last assertion does not prove a slot was freed

`retiring_a_claim_with_a_begun_import_frees_its_slot_and_staged_chunks` calls
`import_reclaim(100 * DAY)` before the retirement. By then the 15 ordinary staging leases
(begun at `DAY` with a 2-day window) have expired. That pass removes their journals, and it
returns 0 only because they staged no chunks.

So the closing `import_begin("after-retirement")` would succeed whether or not the retired
import freed its slot. The `NOT_FOUND` on the retired import and the one reclaimed chunk still
prove D1.

To make the slot assertion meaningful, either:

- run the keep check at an instant before the ordinary leases expire, for example `2 * DAY`; or
- assert, after the retirement reclaim, that a 17th begin was refused right up to that point.

VERDICT: BLOCKED
