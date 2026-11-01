# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

.DEFAULT_GOAL := check
.PHONY: all check tools-check fmt fmt-check lint test census census-cross census-check install-hooks build-runtime

all: check
check: tools-check fmt-check lint test census-check

# Select both executable artifacts explicitly after verification hooks. Some
# managed Cargo targets may republish unselected artifacts from another lease.
build-runtime: tools-check
	cargo build --locked --release -p sparrow-server --bins

tools-check:
	@command -v cargo >/dev/null || { echo "error: cargo is required (Rust 1.98 or newer)"; exit 1; }
	@command -v git >/dev/null || { echo "error: git is required for the pinned PurRDF scan"; exit 1; }
	@command -v tar >/dev/null || { echo "error: tar is required for the pinned PurRDF scan"; exit 1; }
	@command -v capnp >/dev/null || { echo "error: capnp is required to compile the typed RPC schema"; exit 1; }

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --locked --release --workspace --all-targets -- -D warnings

test:
	cargo test --locked --release --workspace

census: tools-check
	cargo run -q --locked --release -p sparrow-helper-census -- --self-test
	cargo run -q --locked --release -p sparrow-helper-census -- --baseline

census-cross: tools-check
	cargo run -q --locked --release -p sparrow-helper-census -- --cross-baseline
	cargo run -q --locked --release -p sparrow-helper-census -- --cross-report > target/helper-census/cross-report.txt
	@head -2 target/helper-census/cross-report.txt

census-check: tools-check
	cargo run -q --locked --release -p sparrow-helper-census -- --self-test
	cargo run -q --locked --release -p sparrow-helper-census -- --cross --check

# Git 2.54's additive hooks preserve existing Stage and repository hooks.
install-hooks:
	git config --local hook.sparrow-verify.event pre-commit
	git config --local hook.sparrow-verify.command 'make check'
	git config --local hook.sparrow-verify.parallel false
