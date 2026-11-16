# Review of #313 at head: renumbered device on the tail and consume paths

Reviewer: independent delivery reviewer (read-only). Written 2026-11-15 17:46 CST.
Head reviewed: `fab12a46cf666d5bf0fed617923ec75d3edb8717` on `paudley/313-start-fails-after-a-reboot-tail`, base `9ceec32`.
Worktree: `/home/paudley/Active/sparrow/.worktrees/313-start-fails-after-a-reboot-tail`. Its tree was clean when I read it.
Commits: `e46bb92` (reviewed PASS in `.stage/313/review.md`), `54394be` (tidy), `fab12a4` (consume path).
I judged `54394be` and `fab12a4`, plus the open findings of the earlier review.

## Scope

Requirement: `.stage/reboot-20261115/issue-tail-device.md` (read verbatim), `.stage/reboot-20261115/recovery-plan.md` and
`.stage/reboot-20261115/issue-stable-identity-v2.md`, with `AGENTS.md`. Neither file has a `## User rulings` section. The
consume-path extension and the claim that "`require` stays unchanged on purpose" come from the caller's brief. I checked
them against the code. They are consistent with the v2 design issue, which lists `CompletedFileIdentity` as pending the
operator's ruling ("No code until he rules").

What I read at head:
- `crates/sparrow-server/src/consumption.rs`: 155-242 (`RootBinding`, `Bound`, `bound`, `require`, `ClaimRoot`),
  286-520 (`open_bound`, `from_root`, `scope`, `private_namespace`, `namespace`), 522-581 (`reserve`, `handoff`,
  `prepare_source`), 583-734 (`recover`, `retire`), 744-760 and 923-1016 (`publish` entry, `validate_claim`,
  `publish_tail`, `publication`, `remove`).
- `crates/sparrow-server/src/tailing.rs`: 51-112 (`TailRoot::open`), 172-198 (lazy `claim`), and 513-562
  (`Declaration`, `check_declaration`, `foreign`).
- `crates/sparrow-server/src/source_file.rs`: 55-130 (`root_binding`, `observe`, `parent`, `directories`).
- `crates/sparrow-server/src/watches.rs`: 95-110 (`identity`), 139-153 (retry constants), 440-545 (`Owner::claim`,
  `Detector` state, `move_held_claim`), 546-660 (`fail_claim`, `status_failure`, `Detector::open`, `reconcile`),
  1088-1103 (startup detector open), 1310-1375 (`admit_handoff`), 1560-1600 (watch change refusal),
  1800-2075 (worker loop: pending selection, reservation, claim recovery and failure handling), 2085-2123 (`ClaimRetry`).
- `crates/sparrow-server/src/local_endpoint.rs` 100-125 and `crates/sparrow-core/src/storage/backup.rs` 215-228. I read
  these only to classify their device comparisons.
- `crates/sparrow-core/src/storage/consumption.rs`: 538-588 (`ConsumptionJournal::validate`), 923-1000
  (`consumption_reserve`), 1133-1213 (`consumption_retire`), 1243-1310 (`consumption_claimed`, `consumption_removed`).
- `crates/sparrow-client/src/imports.rs` 440-484 (`CompletedFileIdentity`, `require`).
- `crates/sparrow-server/src/watches/tests.rs` 1640-1820 (both named tests). I also listed the helpers and store-setup
  sites in that file.
- `docs/interfaces.md` 2700-2713 (the changed paragraph). Commit messages of all three commits; `.stage/313/msg3.txt`.
- A repository-wide `grep` of every `root_device`, `parent_device`, `.device` and `.dev()` use in
  `crates/sparrow-server/src` and `crates/sparrow-core/src`, outside the tests.
- Logs: `.stage/313/test3.log` (whole), `.stage/313/commit3.log` (head, tail and a grep for results, failures and the
  two tests), and the commit and `EXIT` lines of `.stage/313/commit2.log`.

What I did not read: `crash_tests.rs`, `storage/handoff.rs`, `storage/tail.rs` beyond what the earlier review covered,
`agent/tail_finish.rs` beyond its single device comparison, `rpc.rs`, `sparrow-ship`, and the rest of `docs/`. I did not
check the live store. I did not build, run cargo or run any test. Runtime claims below come from the saved logs.

## Evidence

- `.stage/313/test3.log` names the tree `/home/paudley/Active/sparrow/.worktrees/313-start-fails-after-a-reboot-tail` at
  HEAD `54394be`, with the `fab12a4` change not yet committed. It is a release run of the two named tests: 2 passed,
  0 failed, then `EXIT=0` (line 37). Being a filtered run, it covers those two tests only.
- `.stage/313/commit3.log` names the same tree and `54394be`. It is the pre-commit `make check`: fmt, release clippy
  across the workspace and all targets with `-D warnings`, the shipped per-crate clippy, `cargo test --locked --release
  --workspace` (19 `test result: ok` lines, 0 `FAILED` or `panicked` lines), the census self-test and the cross census.
  Both new tests pass (lines 677 and 686). The log ends with the creation of `[... fab12a4]` (line 1580) and `EXIT=0`
  (line 1582).
- `.stage/313/commit2.log` ends with the creation of `54394be` and `EXIT=0` (lines 1572-1574).
- `prose-check --stdin` exited 0 on three inputs: the added `docs/interfaces.md` lines, the added code comments of the
  whole branch, and the three commit messages.

## Design read

### 1. Can `bound()` accept something that leads to deleting, unlinking or importing the wrong file?

`bound` (consumption.rs:184-192) gives `Same` for an equal pair. Under `Stored` it gives `Renumbered` when only the
device differs. Every other case is `Replaced`, including every difference under `Private`. Here is a walk with a
pending journal whose stored `root_device` and `parent_device` are stale and whose inodes are equal:

- `from_root` (334-369): the journal is kept (the `Stored` filter at 349-351) and is not `Replaced`, so the open goes on.
  One operator line is written (365-368). The journal's import id is in `known` (408-411), so its private entry is
  accepted.
- `recover` (585): `scope` (446-467) passes. `namespace` (494-520) passes root and parent at 504-513, or returns at 497
  for a claimed journal. From there the paths split by stage:
  - Reserved, source still at its original path: `require(&file, &journal.observed)` (629) compares all seven fields of
    `CompletedFileIdentity` (imports.rs:477-483), including the device. The file's own device was renumbered too, and
    the store refuses a journal whose observed device differs from its root device (core consumption.rs:540-541).
    So the check fails with `InvalidArgument`. Nothing is renamed.
  - Reserved and renamed into `.sparrow-inflight` but not checkpointed: the fresh observation goes to
    `consumption_claimed` (673). `validate` refuses a claimed device that differs from the observed one (core 577-584,
    CONFLICT). Nothing is adopted.
  - Claimed or published, with the private file present: `require(&file, claimed)` (654) fails.
  - Published, with the private file absent: `publication` (953-978) and `consumption_removed` (601-609) record the
    removal. No file operation happens. In the genuine renumbered case this is the correct completion.
- `validate_claim` (923-933) and `remove` (982-1016) reach `require` on the claimed generation (925, 994, 1001) before
  any upload or `unlinkat` (1004).

What it cannot cause: no path in this code moves, unlinks or uploads a file unless the file matches the stored device,
inode, length, mtime and ctime to the nanosecond. On the same directory, after a renumbering, nothing is touched.

What it can cause when an equal inode sits on a different filesystem (a different mount at the configured path): this
needs the configured root to be a mount point or subvolume top, whose inode is fixed per filesystem type (256 on every
btrfs subvolume, 2 on ext4). Before `fab12a4`, `from_root` refused with "configured watch root was replaced". Now:
1. `.sparrow-inflight` and its lock are created in the other directory (370-407). The open does the same when no claim
   is pending, so this is not new exposure.
2. A published claim whose private copy lives on the original filesystem is found absent. It is then recorded as
   removed (597-609), while its bytes remain in the original filesystem's private directory. No data is lost, because
   the publication is durable. When the original filesystem returns, `from_root` there finds an entry it no longer
   knows and refuses with "unrecognized private claim entry" (420-423). For a finished-files watch that fails startup
   (watches.rs:1096-1101), with an actionable message.
3. A claimed, unpublished claim reports its private source missing (611-615). That makes `retire-claim` admissible
   (consumption.rs:717-733), which an operator might act on while the bytes sit on the other filesystem.

None of these imports or deletes the wrong file. Item 2 can turn a later mount-back into an actionable startup
refusal. See finding O2.

### 2. Can startup still fail, or loop, on a persisted device?

These are every device comparison reachable from startup, classified:

- **Finished-files open:** `open_bound` → `source_file::root` → `from_root` (334-369, now uses `bound`) →
  `private_namespace` (478, 486: descriptors held in this run). Then `reconcile("startup")` → `root_binding`
  (source_file.rs:72: path against held descriptor, same run) → discovery `observe` (source_file.rs:101: file against
  root, same run). No persisted device remains here.
- **Active-files open:** `TailRoot::open` (tailing.rs:60-100) → `foreign` (556-561, exact pair under `Private` only) and
  `check_declaration` (541, device-only gives the gap). The claim root is lazy (tailing.rs:177-190). A refusal there is
  recorded per claim, not at startup.
- **Attach and resume** (tailing.rs:227-292), `reconcile_missing` (500) and the `tail_start` conflict (storage/tail.rs:750)
  use persisted file `(device, inode)` only as lookup keys. A mismatch declares a new generation; it does not refuse.
- **Startup preconditions** (watches.rs:1088-1095) check watch presence, not identity.
- **Not on the startup path:**
  - `admit_handoff` (watches.rs:1322-1375) refuses per request at RPC time.
  - `tail_finish` (agent/tail_finish.rs:118, storage/tail.rs:225 and 662) works per request.
  - The private-intake alias check (watches.rs:99-110), `local_endpoint.rs:110-125` and `backup.rs:222` compare
    devices read in the same run.

The worker loop does not loop tightly. A failing claim follows `ClaimRetry::failed` (watches.rs:2111-2122): 5 s
doubling to a 15 min ceiling (139-153). A held claim's failure returns `Ok` and logs once per change of its text
(525-545). I found no remaining startup refusal on a persisted device. Per the earlier review, a renumbered tail
generation is gapped once and never checked again.

### 3. Where a stranded claim ends up

A claim pending at the reboot opens, and then every recovery fails as walked above. It is:

- Visible. Status carries `claim_failure` with the claim id (watches.rs:2013-2016, 546-569). The durable attempt
  record answers `watch claims` (2018-2030).
- Retried with backoff. Each attempt writes one stderr line (2017). At the ceiling that is about four lines an hour per
  claim. Each attempt also writes the durable attempt record.
- Not blocking the watch's other files. `publishable` takes only claims that are due (1840-1842), and discovery still
  reserves new files. Two side effects remain:
  - The claim keeps one of the 16 global journal slots (core 974-982; watches.rs:1853).
  - It pins the watch's root against change (watches.rs:1576-1583).
  A reserved source still at its original path is rediscovered and reserved again under a new journal. The request id
  includes the observation and directories, so the import id is new. That copy is ingested normally. The old journal
  then fails with NotFound for good.

Ways out, by stage:
- Published, private copy present: no RPC. By hand, unlink the private copy (its bytes are published) and recovery
  records the removal.
- Claimed and unpublished, private copy present: `retire-claim` refuses while the copy exists (723-727). By hand, move
  the copy out, retire the claim and offer the file again.
- Reserved, never moved; or renamed but not checkpointed: there is no way out. `consumption_retire` refuses a claim
  that never checkpointed a move (core 1180-1185). Recovery refuses for good (629 or 673). No RPC deletes a journal. The
  bytes can be taken out by hand, so the data is not stranded, but the journal is: it stays pending, holds a slot and
  pins the root.

This is a state with no way out. It is the residue of leaving `require` unchanged, not something this diff creates. On
the base, the same journals stopped the whole daemon. Removing it needs the persisted file-identity ruling in
`issue-stable-identity-v2.md`. See finding O1. The documentation of this state is wrong; see finding R1.

### 4. Private root, operator lines, tests and hygiene

- **Private root.** Under `Private`, `bound` never returns `Renumbered` (187). `from_root` keeps only `Same` journals
  (349-351), so original-root claims are foreign and never refuse the open. `scope` and `namespace` refuse them
  (453-465, 504-518). `retire` goes through `scope` (715). Correct by reading. No test drives `Private` with a renumbered
  journal; see O3.
- **Operator lines.** `from_root` (365-368) writes one whole line in the `sparrowd: watch=...` form. It names the watch
  and both root devices, taken from the first renumbered journal. It contains no path, payload or secret, and it runs
  once per claim-root open, not on a hot path. The tail line (tailing.rs:88-91) is unchanged since the earlier review.
- **Hygiene.** The production diff has no `unwrap` or `expect`. `Bound`, `bound` and the `binding` field are private.
  `#[must_use]` is on `bound`. Doc comments are on `Bound`, `bound`, `Declaration` and its variants, and
  `check_declaration`. `foreign` has its doc comment back (tailing.rs:548-550), which closes the earlier review's first
  OPTIONAL finding. There is no wire or stored-format change: only server sources and docs changed, with no capnp,
  codec or core storage file.
- **Tests.**
  - `tail_open_gaps_a_renumbered_device_and_refuses_a_changed_inode` (tests.rs:1645-1750) now appends a record before the
    open and asserts that offset and records survive the gap (1734-1749). That makes the "committed records untouched"
    acceptance line meaningful. The refused arm still declares the current device (1710-1715); see O4.
  - `consume_root_accepts_a_renumbered_device_and_refuses_a_changed_inode` (1756-1820) drives the real
    `ClaimRoot::open` and `recover` against a real store. It shows a refusal for a changed inode with a stale device
    (1799-1804). It shows that a device-only difference opens and clears the root and parent checks, and that the file
    is left in place (1806-1819). `!matches!(.., State(_))` is the right discriminator, because every scope and parent
    refusal is `State`.
  - Neither test sleeps. Neither mirrors the arithmetic of `bound`. Each names its invariant in its name and doc
    comment. The inline store setup follows the file's prevailing per-test style; there is no shared helper to reuse.

## Findings

- **R1 (REQUIRED, docs accuracy, introduced by `fab12a4`).** `docs/interfaces.md` 2706-2713 says a pending claim with a
  device-only difference "stays bound and recoverable". It also says only "a claim whose file was already moved into
  the private claim directory refuses its recovery". Both are wrong after a real renumbering, because the file's own
  device changes with its directory's. A reserved claim whose source is still at its original path also refuses
  (consumption.rs:629). The branch's own test asserts exactly that (watches/tests.rs:1806-1819). Only a published claim
  whose private copy is already gone completes. As written, the paragraph tells an operator that unmoved claims recover.
  Fix: say that every pending claim whose source still exists (at its path or in `.sparrow-inflight`) stays pending
  after a renumbering, with its bytes kept and its failure in status. Name the ways out from section 3, or state that
  claims which never checkpointed a move have none until the stable-identity ruling. "Treated alike" also overstates the
  parallel with the tail path, which gaps and continues.
- **O1 (OPTIONAL for this diff; track it).** The state with no way out in section 3 (a reserved or uncheckpointed claim
  that cannot be recovered or retired, pins the root and holds 1 of 16 global slots). It comes from the unchanged
  `require` and is no worse than the base, which refused startup. It should be named in the stable-identity follow-up,
  together with the manual exits for the two claimed stages.
- **O2 (OPTIONAL).** For a root that is a mount point or subvolume top (fixed root inode), `Renumbered` cannot tell a
  different filesystem from a renumbered one. Section 1, item 2: a published claim is then recorded removed while its
  private copy stays on the original filesystem, and the original root later refuses its open on that entry. No data is
  lost and the message is actionable. The v2 design has the same weakness; it is worth one line there.
- **O3 (OPTIONAL, test).** No test covers `Private` with a device-only difference: the open must succeed with the
  journal treated as foreign, and `recover` must refuse it with `State`. One extra `open_bound(.., RootBinding::Private)`
  call in the consume test would cover it.
- **O4 (OPTIONAL, text).** The `54394be` commit message says "the refused arm carries a stale device as well as a
  changed inode". The tail test's refused arm still uses `root_meta.dev()` (tests.rs:1710-1715). Either change the
  declaration to `root_meta.dev() + 1`, or correct the wording in the squash or PR message.
- **O5 (OPTIONAL, helper reuse).** The rule "device-only under `Stored`, exact under `Private`" is now written twice:
  `bound` (consumption.rs:184-192), and `foreign` with the device branch of `check_declaration` (tailing.rs:541-561).
  `TailRoot::open` could classify the root with `consumption::bound`, so the two cannot drift. Census passed, so this is
  advice, not a gate.

The earlier review's finding on the misplaced doc comment is closed, and its test-strength finding is closed in part
(see O4). Its docs finding is addressed for the tail path.

Verdict at head `fab12a46cf666d5bf0fed617923ec75d3edb8717`: one REQUIRED finding (R1, docs accuracy).

VERDICT: BLOCKED
