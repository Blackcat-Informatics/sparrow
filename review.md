# Review of #313: gap a tail generation whose root device was renumbered

Reviewer: independent delivery reviewer (read-only). Date: 2026-11-15.
Head reviewed: `e46bb925b46b5356dfb8f7df559c630b8de06911` on `paudley/313-start-fails-after-a-reboot-tail`,
base `9ceec32`. Worktree: `/home/paudley/Active/sparrow/.worktrees/313-start-fails-after-a-reboot-tail` (clean).
Diff: `crates/sparrow-server/src/tailing.rs` (+58 -9), `crates/sparrow-server/src/watches/tests.rs` (+100).

## Scope

Requirement source: `.stage/reboot-20261115/issue-tail-device.md` (read verbatim), `.stage/reboot-20261115/recovery-plan.md`,
`.stage/reboot-20261115/diagnosis.md`, `.stage/reboot-20261115/issue-stable-identity.md`, and `AGENTS.md`. No
`## User rulings` section exists in these files. The issue text is the contract.

What I read, at head:
- `crates/sparrow-server/src/tailing.rs`: 1-112 (`TailRoot::open`), 152-171 (`root_identity`, `status`), 200-213 (`finish`),
  215-345 (`attach`), 347-488 (`drain`), 490-511 (`reconcile_missing`), 513-556 (`Declaration`, `check_declaration`, `foreign`).
- `crates/sparrow-core/src/storage/tail.rs`: 330-350 (`finished_request`), 387-416 (`registry` and the count ceiling),
  615-845 (`tail_generations`, `tail_finish_get`, `tail_finish_start`, `tail_start`, `tail_generation`, `tail_discontinue`).
- `crates/sparrow-server/src/watches.rs`: 452-463 (`Owner::claim`), 590-635 (`Detector::open`), 648-668 (`reconcile`),
  726-743 (`candidate`), 780-800 (`scan_step` coverage and `reconcile_missing`), 1092-1103 (detector open at startup),
  1464-1503 (`admit_tail_finish`), 1553-1568 (watch change refusal on active generations), 1860-1885 (claim reserve),
  1975-2045 (pending journal recovery and failure handling).
- `crates/sparrow-server/src/consumption.rs`: 285-380 (`open_bound`, `from_root`), 395-410 (`scope`).
- `crates/sparrow-server/src/watches/tests.rs`: 610-692 (existing private-root tail test), 1209-1244 (`tail_detector`),
  1640-1739 (the new test).
- `docs/interfaces.md` 2605-2645 and 2675-2710; `docs/development-runtime.md` 1335-1385 (the 2026-10-17 manual recovery
  of the same watch).
- Evidence: `.stage/313/test.log`, `.stage/313/fmt.log`, and `.stage/313/commit.log`, which I scanned for gate steps,
  failures and the final status. Commit message via `git log -1`.

What I did not read: the rest of `drain` beyond its gap and resume paths, `source_file.rs`, `tail_wire.rs`, `rpc.rs` beyond
380-400, the rest of `consumption.rs`, the rest of `docs/`, `.stage/313/msg.txt`, and the live store. I did not build, run
cargo or run any test. All runtime claims below come from the saved logs.

## Evidence

- `.stage/313/test.log` was run in the worktree at tree base `9ceec32` plus the staged change. A release-profile run of
  `watches::tests::tail_open_gaps_a_renumbered_device_and_refuses_a_changed_inode` gave 1 passed and 0 failed, `EXIT=0`.
- `.stage/313/commit.log` is the pre-commit hook (`make check`) in the same worktree. It ran `cargo fmt --check`, release
  clippy across the workspace and all targets with `-D warnings`, per-crate shipped clippy, `cargo test --locked --release
  --workspace` (19 `test result: ok` lines and no failures in the log), the census self-test and the cross census
  check. It ends with the creation of commit `e46bb92` and `EXIT=0`. The new test appears passing at log line 692.
- I ran `prose-check --stdin` on the commit message and on the added comment lines. Both exited 0.

## Design read (behaviour)

1. **Precedence is preserved.** `TailRoot::open` still skips `complete || gap.is_some()` first (tailing.rs:61) and then
   `foreign()` for the private root (tailing.rs:66), before `check_declaration` (tailing.rs:69). Under `--private-intake`,
   a renumbered original-root generation is still `foreign` and is left untouched, not gapped. `check_declaration`
   (tailing.rs:527-544) evaluates the refusal set (inode, revision, source, format) before the device. A generation with a
   stale device and any other change therefore still refuses with the same `Error::State` text (tailing.rs:93-96).
2. **Offset is correct.** `generation.offset` is the value just read from `tail_generations` (tailing.rs:60), and
   `tail_discontinue` requires `row.offset == offset` (storage/tail.rs:826). Nothing advances a checkpoint between that read and
   the call: no drain runs before `TailRoot::open` returns, and `Detector::open` is the only caller (watches.rs:603). A
   CONFLICT would propagate as `Error::Storage` and is not swallowed. `tail_discontinue` changes only `gap` and `expires_at`
   (storage/tail.rs:836-842). Offset, record count, declaration and start clock are kept, as the issue requires.
3. **No later path fails on the stale device.** I traced each path that reads stored tail generations:
   - `attach` (tailing.rs:236-260) looks for terminal rows by the file's current `(device, inode)` key. The gapped row
     declares the old file device, so it does not match and is not adopted. If the keys did collide, a gapped row with no
     `finish` still falls through (tailing.rs:250-259), because only `complete` or `finish` rows refuse.
   - The resume lookup (tailing.rs:282-286) takes only `!complete && gap.is_none()` rows, so a new generation is declared.
   - The `tail_start` active-identity conflict (storage/tail.rs:744-757) excludes gapped rows.
   - `reconcile_missing` (tailing.rs:494-497) skips gapped rows.
   - `finish` looks up held descriptors by the intent's observed key. A pending finish on a now-gapped row resolves through
     `tail_finish_get` to the original row (watches.rs:1469) and otherwise fails as a CONFLICT for that request only.
   - `status` (tailing.rs:158-171) only lists and sums.
   - The watch change refusal (watches.rs:1559-1562) counts only ungapped rows, so the watch becomes editable again.

   Each further generation of the same watch is checked separately and gapped once. A gapped row is never checked again,
   so a restart cannot loop. The next renumbering reboot gaps only the generations created since. The gapped rows expire
   after 30 days. A full registry falls back to in-memory generations rather than refusing (storage/tail.rs:771-781).
   **No lost data:** with the file's own device renumbered too, the old checkpoint could never be resumed under the current
   stored identity. Without this gap, `reconcile_missing` would have recorded a `read_gap` anyway.
   **Consequence (inference, accepted by the issue and the plan):** each present file is read again from offset 0 under a
   new generation. Lines captured before the reboot are therefore ingested a second time. This is the documented
   outcome of the 2026-10-17 manual recovery (`docs/development-runtime.md` 1349-1384). It is not a blocker here.
4. **Adjacent startup refusal (outside this diff).** `ClaimRoot::from_root` refuses when any pending consumption journal of
   the watch has a different stored `root_device` (consumption.rs:308-320). A finished-files watch opens it eagerly in
   `Detector::open` (watches.rs:594-599). A pending journal on a btrfs root could therefore still stop startup, with a
   different message. For tail watches the claim root is lazy, and a failure there is recorded per claim, not fatally
   (watches.rs:2016-2030). On the observed data this did not trigger: the diagnosis shows `development-completed` reconciling
   before the tail refusal. This belongs to the stable-identity issue, not #313. Before restarting, the primary can confirm
   with a read-only `consumption_pending` check.

## Operator line

tailing.rs:88-91 writes one `eprintln!` line in the daemon's `sparrowd: watch=<id> ...` form. It names the watch, the generation
id and both device numbers. It contains no path contents, payload or secret. It runs once per gapped generation, at most 64 per
watch (the registry ceiling), and only during the open, not on a hot path. It is written after the discontinue succeeds, so the
line never claims a gap that was not recorded.

## Production hygiene

The production diff adds no `unwrap` or `expect`, and no `dbg!` or other print beyond the operator line. Visibility does not widen:
`Declaration` and `check_declaration` are private. `#[must_use]` is on the pure function. The diff touches no wire or stored
format: the gap reason and `tail_discontinue` already exist, and no schema, capnp or codec file changed.

## Test

`tail_open_gaps_a_renumbered_device_and_refuses_a_changed_inode` (watches/tests.rs:1640-1739) names an invariant. It drives the
real `TailRoot::open` against a real store with the `Stored` binding. It has no sleeps and does not mirror the implementation.
It shows both arms: the refusal leaves the generation ungapped (1709-1725), and the device-only change gaps it as
`identity_changed` while the open succeeds (1727-1738). It cannot reuse `tail_detector()` (1209-1244), which opens a detector
over a private root before any declaration exists. The store-setup and `TailStart`/`ConsumeWatch` literals follow the file's
prevailing per-test style, as in 615-692.

## Findings

- **OPTIONAL (text, introduced by this diff): misplaced doc comment.** At tailing.rs:514-517, the new enum was inserted between
  `foreign`'s doc comment and `fn foreign`. As a result, `foreign` (tailing.rs:546) lost its doc, and `Declaration` (519) now
  carries `foreign`'s text ("Whether a generation declared on another root ... never resumed, adopted or closed.") before its
  own. Fix: move lines 514-516 back to directly above `fn foreign`. Clippy does not flag this, which is why the hook passed.
- **OPTIONAL (test strength).** The renumbered generation has offset 0 and 0 records, so the asserts at watches/tests.rs:1737-1738
  hold trivially. They would also pass if the call passed a wrong offset, or if a gap reset the checkpoint to zero. A single
  `store.tail_append(&renumbered.id, 0, b"second line\n", false, now)` before `open()` would make both asserts protect the
  "committed records untouched" acceptance line. Separately, the refusal case changes only the inode while keeping the current
  device, so it does not exercise the new precedence boundary: a stale device together with a changed revision, source or
  format must refuse and not gap. Declaring the refused generation with `root_meta.dev() + 1` and a different `watch_revision`
  (or inode) would cover that boundary with no extra case. The code is correct by reading (tailing.rs:532-544).
- **OPTIONAL (docs, repository rule "keep documentation up to date").** `docs/interfaces.md` 2701-2703 says restart attaches
  surviving identities to their original checkpoints and gaps the unavailable ones. It does not mention the new automatic
  behaviour: a generation whose root differs only by device number gets a durable `identity_changed` gap at open, and the
  present files start new generations from offset 0, so their lines may be ingested again. One sentence there would record
  it. This is not a behaviour blocker under the design-read rule.

No REQUIRED findings. Every acceptance line in the issue is met:
- A device-only change discontinues the generation and the open succeeds.
- A changed inode refuses. Revision, source and format refuse through the same unchanged condition, checked by reading.
- The operator line names the watch and both device numbers.
- The records are untouched by construction (storage/tail.rs:836-842).
- There is no stored-format or wire change.

VERDICT: PASS
