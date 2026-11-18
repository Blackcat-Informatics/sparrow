# Delivery review: issue #366, split the commit hook (recheck)

- Reviewed: 2026-11-18 14:40 CST. Read-only: no edits to the worktree, no cargo, no `make check`, no cargo lock.
- Branch: `paudley/366-split-the-commit-hook-into-a-fast`
- Head: `c8c4e80d690980d82624b0163407cb6a4fcf72d9` (parent `21885ad`, merge base `b2ba715` on main)
- Worktree: `/home/paudley/Active/sparrow/.worktrees/366-split-the-commit-hook-into-a-fast`
- Prior review: `review-21885ad.md` (BLOCKED on R1, R2, R3).

## Scope

Read:
- The issue capture `.stage/split-the-commit-hook-into-a-fast/issue.md` in the worktree (body and acceptance; zero comments).
- `git diff main...HEAD` in full (6 files) and `git diff 21885ad c8c4e80` (5 files).
- The whole `Makefile` and `.github/workflows/ci.yaml` at head.
- Every line that mentions the hook, `install-hooks`, `precommit` or `make check` in tracked Markdown and `.github/`.
- `.stage/366/check.sh`, `commit.sh`, the head and tail of `check3.log` and `commit3.log`, `pr-body.md`, `review-21885ad.md`.

Not read: the GitHub runner image's git version (no network check); the PR as published on the forge;
`docs/development-runtime.md` and `deploy/evidence/` beyond the grep lines (they record past runs).
`check3.log` for this head is still in progress (112 lines, no `EXIT=` line yet); I did not wait for it.

Checks run, all without cargo:
- `make hook-check` in the worktree: exit 0.
- `GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=hook.sparrow-verify.enabled GIT_CONFIG_VALUE_0=false make hook-check`:
  exit 2, message "the sparrow-verify pre-commit hook is not enabled".
- `git hook list pre-commit` (prints `sparrow-verify` on its own line) and with `-c hook.sparrow-verify.enabled=false`
  (prints `disabled<TAB>sparrow-verify`, which `grep -qx sparrow-verify` does not match).
- Scratch repository probe `.stage/366/probe/run.sh` (a fresh `git init` under `.stage/366/probe/repo`, running
  the head `Makefile`'s `hook-check` with `make -f`). Results: unset config exit 2; after `install-hooks` exit 0;
  local `enabled false` exit 2; command `make check` exit 2; event removed exit 2; event `pre-push` exit 2;
  `enabled` unset (git's default is enabled) exit 0.
- `prose-check` on `CONTRIBUTING.md`, `README.md`, `AGENTS.md`: exit 0. `docs/helper-census.md`: exit 1 on line 68
  (`SplitMix64`), which is identical on main and outside this change.
- `git grep` for attribution words in the changed files: only pre-existing `AGENTS.md` authority lines (47-73,
  149, 210, 271); none were added. The diff adds no bypass flag, environment skip, `hooksPath` change or
  hook-disabling setting; `install-hooks` now sets `enabled true` explicitly.

## Requirements

1. **`precommit` is the hook target; `check` keeps the full gate.** `Makefile:13`
   `precommit: tools-check fmt-check lint` and `Makefile:14`
   `check: hook-check tools-check fmt-check lint check-shipped test census-check`. Every gate on main is kept,
   with `hook-check` first. `lint` is release Clippy over `--workspace --all-targets` with `-D warnings`. Met.
2. **`hook-check` fails when the hook is not active.** `Makefile:17-18` compares the local command with
   `make precommit`. `Makefile:19` requires an exact `sparrow-verify` line in `git hook list pre-commit`, which
   git prints only for an enabled hook bound to `pre-commit`. A disabled hook, an old or missing command, and a
   missing or wrong event all fail (probe above). Git's own resolution of `enabled` across scopes decides the list,
   so a global or system `enabled false` also fails. A failing `git hook list` (for example a git without the
   subcommand) leaves grep with no input, so the target fails closed. Met; prior R2 resolved.
3. **`install-hooks` sets `make precommit` and keeps event and parallel.** `Makefile:83-86` sets event
   `pre-commit`, command `make precommit`, parallel `false`, and adds `enabled true`. Met.
4. **CI runs `make install-hooks` before `make check`.** `ci.yaml:75-81`. `actions/checkout` with `path: sparrow`
   creates an ordinary repository at `sparrow/.git`. `git config --local` there writes `sparrow/.git/config`, and
   `git hook list pre-commit` in `sparrow` reads it. The runner has no global hook config, so the list prints
   `sparrow-verify` and `hook-check` passes, provided the runner's git is 2.54 or newer (see O1). Met; prior R1
   resolved.
5. **Docs no longer say the hook runs `make check`.** `CONTRIBUTING.md:72,88`, `README.md:149`,
   `docs/helper-census.md:32` and `AGENTS.md:184-186` name `make precommit`. The remaining `make check` mentions
   near hooks are `docs/deployment.md:19` (run the gate, then commit through the hook: still accurate) and
   historical run records in `docs/development-runtime.md`. Met; prior R3 resolved.
6. **No verification bypass.** None in the diff (see checks). Met.
7. **No quotations or attributed requirements added.** Met.

## Findings

No REQUIRED findings.

**O1 OPTIONAL: CI now depends on the runner's git having `git hook list` (2.54 or newer).** Before this change
CI never touched hooks. On an older runner git, `Makefile:19` fails with "not enabled", which misnames the cause.
`deploy/README.md:187` already documents git 2.54 as a requirement. A version check in `tools-check`, or a
`git --version` line in the workflow, would make a failure self-explaining. Not verified against the current
`ubuntu-latest` image.

**O2 OPTIONAL: `CONTRIBUTING.md:88` is 111 columns** while the surrounding paragraph wraps near 80; `AGENTS.md:186`
is 85.

**O3 OPTIONAL (carried from the prior review, operational):** the shared `.git/config` already sets the hook
command to `make precommit`, so sibling worktrees whose Makefile lacks the target fail in the hook until they
merge main. This fails closed. The PR body line "once this lands every checkout needs the `precommit` target"
still understates that.

**O4 OPTIONAL:** AGENTS.md still says "every cargo command ... goes through `flock`" while the hook runs cargo fmt
and clippy without it; the hook sentence's "no lock" resolves it for a careful reader.

## Open acceptance state (not a code defect; complete before merge)

- `make check` on `c8c4e80` is running (`check3.log`, no `EXIT=` yet). The issue requires it to pass on the
  merging head and be cited in the PR body.
- `pr-body.md` still cites head `21885ad` and describes `hook-check` as a command comparison only. Update it for
  the `enabled` check, the CI step and the new head's `EXIT` line.
- The warm-cache commit timing in the PR body (24.9 s, `commit2.log`) meets the 60 s bound. The latest commit
  (`commit3.log`, creating `c8c4e80`) took 82.8 s with a 71 s Clippy step; that run was not warm and is not a
  regression in the change, but do not cite it as the warm figure.

VERDICT: PASS
