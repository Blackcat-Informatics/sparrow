# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

.DEFAULT_GOAL := check
.PHONY: all check precommit hook-check tools-check fmt fmt-check lint test check-shipped census census-cross census-check install-hooks build-runtime install-bin seed-excludes-check

all: check

# Two gates. `precommit` is what the pre-commit hook runs: tool presence,
# formatting and the release clippy over all targets; no tests and no cargo
# lock. `check` is the full gate (hook-check first, so a checkout without the
# hook cannot pass silently); it must pass on a change's head before merge.
precommit: tools-check fmt-check lint
check: hook-check tools-check fmt-check lint check-shipped test census-check

hook-check:
	@cmd="$$(git config --local --get hook.sparrow-verify.command)"; \
	test "$$cmd" = 'make precommit' || { echo "error: the pre-commit hook command is '$${cmd:-unset}', expected 'make precommit'; run 'make install-hooks'"; exit 1; }
	@git hook list pre-commit | grep -qx sparrow-verify || { echo "error: the sparrow-verify pre-commit hook is not enabled in this checkout; run 'make install-hooks'"; exit 1; }

# Select both executable artifacts explicitly after verification hooks. Some
# managed Cargo targets may republish unselected artifacts from another lease.
build-runtime: tools-check
	cargo build --locked --release -p sparrow-access -p sparrow-server --bins

# Keep the deployed images and lexicons in ./bin under revision-qualified names
# (tools/install-bin.sh); deploy/install.sh installs from there.
install-bin: tools-check
	@git diff --quiet HEAD -- || { echo "error: commit first; ./bin names images for HEAD"; exit 1; }
	cargo build --locked --release -p sparrow-access -p sparrow-server --bins --example recovery --example stamp
	tools/install-bin.sh

# Every exclusion in .config/wt.toml names something this host keeps (tools/seed-excludes-check.sh).
seed-excludes-check:
	tools/seed-excludes-check.sh

tools-check:
	@command -v cargo >/dev/null || { echo "error: cargo is required (Rust 1.98 or newer)"; exit 1; }
	@command -v git >/dev/null || { echo "error: git is required for the pinned PurRDF scan"; exit 1; }
	@command -v tar >/dev/null || { echo "error: tar is required for the pinned PurRDF scan"; exit 1; }
	@command -v python3 >/dev/null && python3 -c 'import tomllib' 2>/dev/null || { echo "error: python3 3.11 or newer (tomllib) is required for tools/purrdf-lexicons.sh"; exit 1; }
	@command -v capnp >/dev/null || { echo "error: capnp is required to compile the typed RPC schema"; exit 1; }

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --locked --release --workspace --all-targets -- -D warnings

test:
	cargo test --locked --release --workspace

# The workspace gates unify features, so sparrow-core's test-support feature is on in them and
# hides warnings a shipped build hits. Check each shipped crate on its own, one cargo invocation
# per crate so no other package's features unify in, with warnings denied and no test-support.
check-shipped:
	cargo clippy --locked --release -p sparrow-access --lib --bins -- -D warnings
	cargo clippy --locked --release -p sparrow-limits --lib -- -D warnings
	cargo clippy --locked --release -p sparrow-core --lib -- -D warnings
	cargo clippy --locked --release -p sparrow-client --lib -- -D warnings
	cargo clippy --locked --release -p sparrow-server --lib --bins -- -D warnings

# The census defaults to the workspace its binary was compiled in. A shared
# compile cache can reuse a binary built in another checkout, so name this one.
CENSUS := cargo run -q --locked --release -p sparrow-helper-census -- --root $(CURDIR)

census: tools-check
	$(CENSUS) --self-test
	$(CENSUS) --baseline

census-cross: tools-check
	$(CENSUS) --cross-baseline
	$(CENSUS) --cross-report > target/helper-census/cross-report.txt
	@head -2 target/helper-census/cross-report.txt

census-check: tools-check
	$(CENSUS) --self-test
	$(CENSUS) --cross --check

# Git 2.54's additive hooks preserve existing Stage and repository hooks.
install-hooks:
	git config --local hook.sparrow-verify.event pre-commit
	git config --local hook.sparrow-verify.command 'make precommit'
	git config --local hook.sparrow-verify.parallel false
	git config --local hook.sparrow-verify.enabled true
