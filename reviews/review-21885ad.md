# Delivery review: PR #368 (issue #366), split the commit hook

- Reviewed: 2026-11-18 10:11 CST, read-only (no edits, builds or commits)
- Branch: `paudley/366-split-the-commit-hook-into-a-fast`
- Head: `21885adae9acecf92cf4810f730b36ba8430bf4a` (parent `2e1eafd`, merge base `b2ba715`)
- Worktree: `/home/paudley/Active/sparrow/.worktrees/366-split-the-commit-hook-into-a-fast`

## Scope

Read:
- `stagectl issue 366` (body and acceptance; forge comments were unavailable because the forge timed out).
- `git diff origin/main...HEAD` in full (`Makefile`, `AGENTS.md`).
- The whole `Makefile` at head; `.github/workflows/ci.yaml`.
- `git help config` entries for `hook.<friendly-name>.{command,event,enabled,parallel}` (git 2.56.1).
- Evidence under `/home/paudley/Active/sparrow/.stage/366/`: `check.sh`, `check2.log`, `commit.sh`,
  `commit.log`, `commit2.log`, `precommit.log`, `pr-body.md`.
- Every line that mentions the hook or `make check` in `CONTRIBUTING.md`, `README.md`,
  `docs/helper-census.md`, `deploy/README.md` and `.github/PULL_REQUEST_TEMPLATE.md`.

Not read: the rest of `check.log` and `check2.log` beyond the head, tail, gate lines and failure grep;
`docs/development-runtime.md` and `deploy/evidence/gate.md` (historical records of past runs, not the
current contract); the PR as published on the forge.

Checks run (all read-only): `make -n precommit`, `make -n check`, `make hook-check` (exit 0),
`git config --show-origin --show-scope --get-regexp '^hook\.'`, `git hook list pre-commit`,
`git -c hook.sparrow-verify.enabled=false hook list pre-commit`, `git show <rev>:Makefile` for the two
sibling worktree heads, `git grep -i -E 'patrick|ruling|ruled'`, `prose-check AGENTS.md` (exit 0) and
`prose-check pr-body.md` (exit 0).

## Evidence against the caller's criteria

1. `precommit: tools-check fmt-check lint`. Per `make -n`, it runs the five tool checks,
   `cargo fmt --all --check` and `cargo clippy --locked --release --workspace --all-targets -- -D warnings`.
   It runs no test suite, no `check-shipped`, no census and takes no flock. The issue's "document checks"
   are covered because the workspace lints set `missing_docs = "warn"` and `-D warnings` promotes it.
   `check: hook-check tools-check fmt-check lint check-shipped test census-check` is the origin/main list
   with `hook-check` prepended; nothing was removed. Met.
2. `hook-check` compares `git config --local --get hook.sparrow-verify.command` with `make precommit`.
   A missing key yields an empty string and fails with an actionable message, and so does the old
   `make check` value. It does not pass vacuously in that case. It checks neither `event` nor `enabled`;
   see finding R2.
3. `install-hooks` changes only the command value to `make precommit`; `event pre-commit` and
   `parallel false` are unchanged. It writes `--local`, which in a linked worktree (no
   `extensions.worktreeConfig`) is the shared `/home/paudley/Active/sparrow/.git/config`. That file is what
   `git hook list pre-commit` reads: it lists `sparrow-verify`. Met.
4. AGENTS.md: the only changed lines are the hook sentence (lines 184-186) and the cargo-lock sentence
   (lines 191-192). The new hook text matches `precommit` and requires `make check` on the head before
   merge, cited by its EXIT line, as the issue asks. The lock sentence drops "and commit". Both are
   grammatical, and `prose-check` passes. Met, with optional nits O2 and O3.
5. `check2.log` line 1 names the worktree and line 2 names `21885adae9acecf92cf4810f730b36ba8430bf4a`.
   It shows the three `check-shipped` clippy runs, 20 `test result: ok` lines, no FAILED line, both
   census steps, and ends `EXIT=0`. `check.sh` captures `make check`'s own status with no pipe.
   `commit2.log` names the worktree and the pre-commit HEAD `2e1eafd` (the parent of `21885ad`). The hook
   ran fmt and clippy, the commit exited 0, and WALL is 24.899 s, under the 60 s acceptance bound.
   `commit.log` records the first commit at 43.6 s, which includes cargo package-cache lock waits.
   The evidence is honest. Met.
6. `git grep -i -E 'patrick|ruling|ruled'` matches only `AGENTS.md` (10) and `CITATION.cff` (1). Met.

## Findings

**R1 REQUIRED: the shipped CI gate now fails on every run.** `.github/workflows/ci.yaml` runs
`make check` on a fresh `actions/checkout` and never runs `make install-hooks`. With `hook-check` first,
`git config --local --get hook.sparrow-verify.command` is empty there, so the job fails before any real
gate runs, on every pull request and every push to main on github.com. The diff turns a working gate into
one that is always red, which hides real results. Fix: add a `make install-hooks` step before
`make check` in the workflow. Exempting CI from `hook-check` would also work, but the extra step is
smaller and keeps one path.

**R2 REQUIRED: `hook-check` passes when the hook is disabled or bound to no event.** Git reads three keys
to decide whether the hook runs, and `hook-check` reads only one. Git documents
`hook.<name>.enabled = false` as the switch that disables a hook "without removing its configuration".
With it set, `git -c hook.sparrow-verify.enabled=false hook list pre-commit` prints
`disabled	sparrow-verify`, but the `--local --get ...command` read still returns `make precommit`. A
locally set `enabled false`, or a missing or changed `event`, therefore leaves commits unverified while
`make check` passes. That is the "hook that can pass uninstalled" case. Fix: also require that
`git hook list pre-commit` prints exactly `sparrow-verify` as an enabled entry, keeping the command
comparison. Reading the effective config without `--local` for `enabled` would also work.

**R3 REQUIRED: maintained docs still say the hook runs `make check`.** AGENTS.md requires repository
documentation to change with the behaviour, and these passages are now false:
- `CONTRIBUTING.md:72`: "make install-hooks  # run make check as an additive pre-commit hook"
- `CONTRIBUTING.md:87-88`: "...configuration to run `make check` before every commit"
- `README.md:149`: "make install-hooks  # run make check before every commit"
- `docs/helper-census.md:32`: "It runs `make check` on commits"

Each needs a one-line correction to `make precommit`. `deploy/evidence/gate.md` and
`docs/development-runtime.md` record past runs and need no change.

**O1 OPTIONAL (operational, not a diff defect): the new hook command is live in sibling checkouts now.**
The hook command lives in the shared `.git/config` and already reads `make precommit`. The Makefiles at
the heads of `.worktrees/365-show-the-archive-worker-s-phase` (`2b42a8e`) and
`.worktrees/gate-hold-unarchived` (`ca6cff4`) have no `precommit` target. A commit there now fails in the
hook with "No rule to make target". It fails closed, so no verification is skipped, but those lanes cannot
commit until this lands and they merge main. The PR body says "once this lands every checkout needs the
`precommit` target", which understates it: the requirement already applies. Reword that line, and tell
the owners of the sibling lanes.

**O2 OPTIONAL:** AGENTS.md line 186 runs to about 87 columns while the surrounding text wraps near 80.

**O3 OPTIONAL:** the lock sentence still says "every cargo command ... goes through `flock`", while the hook
runs cargo fmt and clippy unlocked. The hook sentence's "no lock" resolves this for a careful reader. Saying
"every cargo command outside the commit hook" would remove the tension.

**O4 OPTIONAL:** the hook-check error text says "not installed" even when an outdated command
(`make check`) is installed. The remedy it names is still correct.

VERDICT: BLOCKED
