# Issue #366: Split the commit hook into a fast precommit and the full make check

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Milestone: Before PREFLIGHT.

The commit hook runs the full `make check` (about 3 to 28 minutes per commit), so every review round and fix waits on it. Split it:

- `make precommit`: tools-check, fmt-check and the release clippy over all targets, with document checks. No test suite, no check-shipped build, no cargo lock. The pre-commit hook runs it.
- `make check` keeps every gate, tests included, and must pass on a change's head before it merges. Its result is cited in the pull request from its own EXIT line.
- The checkout must verify that the hook is installed: `make install-hooks` config is present, and the gate says so when it is missing.

## Acceptance

- A commit finishes in under 60 seconds on a warm cache, measured and recorded in the pull request.
- `make check` is unchanged and passes on the merging head, cited in the PR body.
- AGENTS.md no longer says the hook runs the full gate, and no longer requires the cargo lock around commits; it names the split.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
