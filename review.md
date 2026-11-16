# Independent review: #314, sparrow-ship ledger key without the device number

- Head reviewed: `d100b7f3eeb44857ed2b7cfc66c189e1806f9abc` (checked with `git rev-parse HEAD` at start and at 2026-11-15 18:53:16 -0600), branch `paudley/314-sparrow-ship-re-ships-every-source-file`, base `fda2812`, one commit, worktree clean.
- Worktree: `/home/paudley/Active/sparrow/.worktrees/314-sparrow-ship-re-ships-every-source-file`. Nothing in it was modified; no cargo or build was run.

## Scope

Read:
- `git diff fda2812..d100b7f` in full (4 files, +85/-7).
- `deploy/libexec/sparrow-ship` in full (1-338): `identity` 104-105, `ledger_keys` 108-120, `finished` 132-158 (daily/sealed/settled), `enqueue` 189-232 (its own `identity` comparison at 221), `feed` 235-275, `run` 278-331 (ledger read 301-304, key 308, append 323-325).
- The installed older copy `~/.local/libexec/sparrow/sparrow-ship`, lines 28, 83, 185, 265-289 (its ledger parse at 268).
- `crates/sparrow-server/tests/rpc.rs` 7077-7205: the `ship()` helper (7078), the neighbour test (7091) and the new test (7147-7201).
- `docs/deployment.md` 388-460; `deploy/libexec/katamari-manager-spool` 13-19; `docs/development-runtime.md` 5436-5446 and 6360-6375 (to classify grep hits).
- `.stage/314/focused.log` and `.stage/314/commit.log` (head, tail and key lines), plus the branch reflog and the repository's `sparrow-verify` pre-commit hook entry (`make check`).
- `.stage/step4/comment-314.md` and the #314 section of `.stage/STATUS.md`.
- The live ledgers under `~/.local/state/sparrow/ship/*.ledger`, read only: field counts of each key.

Not read:
- The verbatim #314 issue body and comments: `gh` cannot reach the Forgejo remote, and no `issue.md`, `plan.md` or `validation.md` exists under `.stage/314`. The requirement was taken from the caller's brief and checked against `comment-314.md`.
- The rest of `rpc.rs`, the rest of `docs/`, and `deploy/README.md` beyond the grep below.

## Evidence

- Reviewer probe (not cargo, outside the worktree): `.stage/314/review-probe.sh` ran the worktree script against scratch trees under `.stage/314/review-probe/`; log `.stage/314/review-probe.log`, EXIT=0. Observed: missing ledger ships 1 and writes a three-field line; a rerun queues 0; an empty ledger ships 1; a blank line inside the ledger and a five-field key each exit 1 naming `ledger path:line`; a `:` in a file name ships and a rerun queues 0; three non-numeric fields (`a:b:c`) are accepted silently; a carriage return in a file name ships, then the next run exits 1 at ledger line 3.
- Live ledgers: all six are entirely four-field keys (refusals 68,707 lines), no blank lines, final byte a newline, so the new parser reads each of them without a failure.
- `focused.log`: names the worktree, HEAD `fda2812` (the change was uncommitted), release profile, `tests/rpc.rs` filtered to the 3 shipper tests, all three ok including `shipper_never_reships_a_file_whose_device_number_alone_changed`; ends `EXIT=0`.
- `commit.log`: names the worktree and HEAD `fda2812`, the full `make check` output (fmt, release Clippy with `-D warnings`, shipped crates, release tests with the new test ok at line 1084, census self-tests, the pinned cross gate with `--root` set to this worktree); ends `EXIT=0`. It does not print the resulting hash. It is tied to `d100b7f` by the reflog: the only commit after `fda2812` is `d100b7f` at 18:30:49, which is the start time git caches for author, committer and reflog dates before the pre-commit hook runs; the log closed at 18:45:58, when the hook finished. That is consistent with the hook's own `make check` verifying this commit.
- `prose-check` on the changed docstring, the `deployment.md` paragraph and the test doc comment: EXIT=0.

## Check by check

1. One implementation. Every ledger read goes through `ledger_keys` (sparrow-ship:304), the only reader in the repository (grep of tracked files for `.ledger` readers). The writer appends `identity(stat)` (308, 323), the same function the reader's output is compared against. Split: the key is `line.split("\t", 1)[0]`, so a `:` or tab in the relative path never reaches the key parse (probe confirmed for `:`). Four fields drop the leading device with `fields[1:]`, giving `inode:size:mtime_ns`, which is byte-equal to `identity()` for the same stat, so a legacy line and a new line for one file collide. No off-by-one. Missing ledger: `exists()` guard (303). Empty ledger: no lines. A trailing newline produces no empty element under `splitlines()`. A blank line in the middle now fails loudly (it was a harmless empty key before); the writer never produces one.
2. Behaviour risks:
   - A changed file: a different size or mtime_ns gives a different key, so it ships again. Confirmed by reading and by the test's last step.
   - An unchanged file is not re-shipped across a renumbered device. That is the fix, confirmed by the test and by the probe rerun.
   - Collisions within one source: two distinct live files on one filesystem never share an inode, so they cannot collide. Hard links are one file and shared the key before as well; the second name is skipped, as before. Inode reuse after a delete and create with an equal size and mtime_ns is undetected, as before (the device was equal in that case too), and the docs say so. Every live source uses a flat pattern in one directory, so it cannot cross a mount. See OPTIONAL 3 for recursive patterns.
   - Daily, sealed and settled rules: `finished` is unchanged by the diff.
   - `enqueue`'s copy check (221) now ignores the device. Both stats come from one run, so nothing is lost.
   - Rollback to the installed older script: it builds a plain set of first-column strings (installed:268) with no parsing, so three-field lines simply never match its four-field `identity`. It re-ships those files once and appends four-field lines; it does not crash. The new script reads those four-field lines as the same entries afterwards.
3. Test (rpc.rs:7147-7201). It drives the real script through the existing `ship()` helper, uses no sleeps, and uses `settle_seconds = 0`. "queued N" is read from the script's own stdout line `sparrow-ship: reports: queued N` (sparrow-ship:328). It fails on the old code: the old script writes four-field lines, the test prefixes `4242:` to make a five-field key, and the old reader's raw first column never equals the four-field identity, so `kept.json` would be queued and the `queued 0` assertion fails. A half fix (a new `identity` with the old reader) also fails, because `4242:ino:size:mtime` is not the three-field identity. The ledger mixes both forms (`other.json` stays three-field). The `contains("\n4242:")` assertion is a non-vacuity guard (without it a missing rewrite would still pass `queued 0`), not a mechanical check. The final rewrite changes the size (and mtime), and `queued 1` follows.
4. Docs: the sparrow-ship docstring (28-37), `deployment.md` 398-403 and `katamari-manager-spool:16` are accurate. `docs/deployment.md:448` is not (REQUIRED 1). Other `device`/`inode` hits in `docs/development-runtime.md` and `docs/interfaces.md` describe the daemon's consumption and tailing identities, not this ledger.
5. Logs: both carry their own `EXIT=0` and name the right tree. They name the base hash, which was HEAD when each run began. The tie of `commit.log` to `d100b7f` is shown above.

## Findings

**REQUIRED 1: stale ledger-key statement in the operator docs.** `docs/deployment.md:448` still reads "`sparrow-ship` keys a file by device, inode, size and modification time and ships a changed file again whole". The commit corrected the same sentence in `deploy/libexec/katamari-manager-spool:16` and the paragraph at `deployment.md:398` above, so the document now contradicts itself. The brief requires the docs to be updated, and AGENTS.md says documentation changes with the behaviour. Fix: drop "device," from that line. This is text, not a design point; no code change is needed.

**OPTIONAL 1: a line-break character in a source file name now stops every run.** `ledger_keys` uses `str.splitlines()`, which also splits on `\r`, `\v`, `\f`, `\x1c`-`\x1e`, `\x85`, U+2028 and U+2029, while the writer only ends lines with `\n` and writes the name raw (323). The probe showed that a file named with `\r` ships, and then the next run exits 1 at the fragment line. Because `fail()` is `sys.exit`, every later source and `feed()` stop too until an operator removes the fragment. A literal `\n` in a name does the same. Before this change the fragment was a harmless unmatched key. This is the required loud failure applied to a line the shipper wrote itself, so it does not block; the operator gets the path and line number as a way out, and live producers use hash and date names. Smallest hardening: split on `"\n"` only (closes every case except `\n`), or escape the name on write in a later change.

**OPTIONAL 2: "malformed" checks shape only.** Three or four fields with non-numeric or empty parts (`a:b:c`, `::`) pass silently and never match, so that file ships again. No data is lost. Validating each field as a decimal integer would make the loud-failure rule complete.

**OPTIONAL 3: recursive patterns.** `Path.glob` accepts `**`. A recursive pattern that crosses into a btrfs subvolume or snapshot inside a source can see two distinct files with equal inode, size and mtime_ns (a snapshot copies all three). The device used to separate them; now the second is skipped. No configured source does this (all patterns are flat). A sentence in the docs, or refusing `**` in `load_config`, would close it.

**OPTIONAL 4: test scaffolding and diagnostics.** The config TOML literal at rpc.rs:7163-7177 is a third copy of the one at 7116-7130 (and the overlap test's). A small shared builder would follow the "one dev-only home" rule. The three `assert!(report(..).contains(..))` calls print nothing on failure; the neighbour test passes `"{report}"` as the message.

**OPTIONAL 5: prose.** The inserted sentences separate "A crash between the two" (`deployment.md:403`) and "A crash between those steps" (sparrow-ship:33) from their antecedent ("durably queued" and "records"), and `deployment.md:403` is an over-long source line.

VERDICT: BLOCKED
