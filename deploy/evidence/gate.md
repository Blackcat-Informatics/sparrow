<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# The make check gate (issue #22, first automated acceptance item)

The pre-commit hook runs `make check` on every commit, so a commit's hook log
is its gate record; `make check` was not run separately. The record below is
the hook of commit `74d4cf0` ("Address the deploy review: archive path, schema
rollback, install order"), the last commit on the branch that changes
anything other than evidence, run on 2026-11-02. Its full output is kept
outside the repository at
`~/.local/state/sparrow-deliver/gates/deploy-review-fixes.log` (973 lines).
The commit that adds this file runs the same hook; its result is reported
with it, not here.

Scope, as `make check` defines it: formatting of the whole workspace, release
Clippy over every target with warnings denied, the release tests of the whole
workspace, the helper census self-test and the pinned cross-workspace census
check. The commands and every pass or fail line, verbatim:

```text
cargo fmt --all --check
cargo clippy --locked --release --workspace --all-targets -- -D warnings
    Finished `release` profile [optimized] target(s) in 0.87s
cargo test --locked --release --workspace
    Finished `release` profile [optimized] target(s) in 0.68s
test result: ok. 22 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s
test result: ok. 33 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.24s
test result: ok. 201 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 22.87s
test result: ok. 50 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 23 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 71.40s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.36s
test result: ok. 72 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 19.89s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
cargo run -q --locked --release -p sparrow-helper-census -- --root /home/paudley/Active/sparrow/.worktrees/deploy --self-test
OK: helper-census self-test
cargo run -q --locked --release -p sparrow-helper-census -- --root /home/paudley/Active/sparrow/.worktrees/deploy --cross --check
OK: 305 ledger jobs and 1 [[distinct]] rows hold; no unsanctioned isomorphic group
[deploy 74d4cf0] Address the deploy review: archive path, schema rollback, install order
```

Result: every stage passed; the release tests report 414 passed and 0 failed
across the workspace's test binaries, and git recorded the commit, which the
hook allows only after `make check` exits 0. The Clippy and test builds
finished in under a second because the hooks of the earlier commits had built
the same sources.

The two earlier attempts at commit `d9bf340` are on record too: the first
hook failed two tests in `crates/sparrow-server/tests/rpc.rs` with
`AddrInUse` (a TCP port the tests reserve and release was taken by another
process on the host before the daemon bound it; the change touched no Rust),
and the second passed.
