#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# `make install-bin`: keep the deployed images in ./bin under revision-qualified names, so a
# deployed image always has a source copy after target/ is culled. Copies are reflinks
# (cp --reflink=auto); the release build itself runs through the managed cargo wrapper first.
#
#   bin/sparrowd-schemaN-REV          the daemon
#   bin/sparrow-schemaN-REV           the CLI
#   bin/sparrow-recovery-schemaN-REV  the restored-store probe (deploy/RECOVERY.md)
#   bin/lexicons-PURRDF_REV/          the five native CBOR lexicons, with provenance and licenses
#
# N is sparrow_client::SCHEMA_VERSION and REV the short commit. The tree must be clean, since
# the names claim the commit; an existing name is never overwritten with different bytes.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

git diff --quiet HEAD -- || { echo "install-bin: commit first; ./bin names images for HEAD" >&2; exit 1; }
schema=$(sed -n 's/^pub const SCHEMA_VERSION: u32 = \([0-9]*\);/\1/p' crates/sparrow-client/src/lib.rs)
rev=$(git rev-parse --short=7 HEAD)
[[ -n $schema ]] || { echo "install-bin: SCHEMA_VERSION not found" >&2; exit 1; }

keep() {
    local source=$1 target=$2 staged
    if [[ -e $target ]]; then
        cmp -s -- "$source" "$target" || { echo "install-bin: $target exists with other bytes" >&2; exit 1; }
        echo "install-bin: kept $target"
        return
    fi
    staged=$(mktemp bin/.staged.XXXXXX)
    cp --reflink=auto -- "$source" "$staged"
    chmod 0755 -- "$staged"
    cmp -s -- "$source" "$staged" || { rm -f -- "$staged"; echo "install-bin: copy of $source differs" >&2; exit 1; }
    mv -T -n -- "$staged" "$target"
    echo "install-bin: $target"
}

install -d -m 0755 bin
keep target/release/sparrowd "bin/sparrowd-schema$schema-$rev"
keep target/release/sparrow "bin/sparrow-schema$schema-$rev"
keep target/release/examples/recovery "bin/sparrow-recovery-schema$schema-$rev"

# The lexicons come from the PurRDF revision Cargo.lock resolves, as cargo checked it out.
purrdf=$(cargo metadata --format-version 1 --locked | python3 -I -c '
import json, sys
from pathlib import Path
packages = json.load(sys.stdin)["packages"]
lex = next(p for p in packages if p["name"] == "purrdf-lex")
print(Path(lex["manifest_path"]).parents[2], lex["source"].rsplit("#", 1)[-1])')
read -r checkout purrdf_rev <<<"$purrdf"
lexicons=$checkout/crates/text/lexicons
[[ -d $lexicons/artifacts ]] || { echo "install-bin: no lexicons at $lexicons" >&2; exit 1; }
target=bin/lexicons-$purrdf_rev
if [[ -d $target ]]; then
    diff -r -q -- "$lexicons" "$target" >/dev/null || { echo "install-bin: $target differs from $lexicons" >&2; exit 1; }
    echo "install-bin: kept $target"
else
    staged=$(mktemp -d bin/.lexicons.XXXXXX)
    cp -a --reflink=auto -- "$lexicons/." "$staged/"
    diff -r -q -- "$lexicons" "$staged" >/dev/null || { echo "install-bin: copy of $lexicons differs" >&2; exit 1; }
    chmod 0755 -- "$staged"
    mv -T -n -- "$staged" "$target"
    echo "install-bin: $target"
fi
