#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Materialise the PurRDF text lexicons of the locked release under BINDIR/lexicons-<revision> and
# print that directory on stdout. The registry crates exclude them (`exclude = ["lexicons/**"]`),
# so they come from the PurRDF repository, archived with `git archive`, never from a working tree.
#
# The revision is registry/compatibility.toml's purrdf `git_revision`. It is used only when the
# registry `version` equals the `purrdf-text` version Cargo.lock locks and the revision equals the
# `git.sha1` in the locked crate's `.cargo_vcs_info.json`; otherwise the script refuses. An
# existing destination must hold the same bytes; it is never replaced.
#
#   tools/purrdf-lexicons.sh ROOT BINDIR
#
# ROOT is the canonical checkout root: it holds the sibling PurRDF repository the registry names.
# BINDIR is the bin directory that receives the bundle (created when missing): `make install-bin`
# passes the checkout's own ./bin, beside the images; sparrow-test-support passes the canonical
# ROOT/bin. The registry, Cargo.lock and manifests are read from the checkout this script lives in,
# so a linked worktree under ROOT/.worktrees/ is judged by its own lock, not the main checkout's.
set -euo pipefail

(($# == 2)) || { echo "usage: purrdf-lexicons.sh ROOT BINDIR" >&2; exit 2; }
root=$(cd -- "$1" && pwd) || { echo "purrdf-lexicons: ROOT $1 is not a directory" >&2; exit 2; }
mkdir -p -- "$2"
bindir=$(cd -- "$2" && pwd) || { echo "purrdf-lexicons: BINDIR $2 is not a directory" >&2; exit 2; }
checkout=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
for needed in registry/compatibility.toml Cargo.lock Cargo.toml; do
    [[ -f $checkout/$needed ]] || { echo "purrdf-lexicons: $checkout/$needed is missing" >&2; exit 2; }
done

metadata=$(cd -- "$checkout" && cargo metadata --locked --format-version 1) \
    || { echo "purrdf-lexicons: 'cargo metadata --locked' failed in $checkout; the lock and the manifests must agree" >&2; exit 1; }

read -r repository revision < <(printf '%s' "$metadata" | python3 -I -c '
import json, sys, tomllib
root = sys.argv[1]
def refuse(message):
    sys.exit("purrdf-lexicons: " + message)
with open(root + "/registry/compatibility.toml", "rb") as f:
    supplier = tomllib.load(f).get("supplier", {}).get("purrdf")
if not isinstance(supplier, dict):
    refuse("registry/compatibility.toml must contain [supplier.purrdf]")
try:
    repository, revision, version = supplier["repository"], supplier["git_revision"], supplier["version"]
except KeyError as missing:
    refuse("the registry purrdf supplier has no " + str(missing))
with open(root + "/Cargo.lock", "rb") as f:
    locked = sorted({p["version"] for p in tomllib.load(f)["package"] if p["name"] == "purrdf-text"})
if len(locked) != 1:
    refuse("Cargo.lock must lock exactly one purrdf-text version, found " + repr(locked))
if version != locked[0]:
    refuse("registry/compatibility.toml records purrdf " + version + " but Cargo.lock locks purrdf-text "
           + locked[0] + "; update the supplier version and git_revision to the locked release")
crates = [p for p in json.load(sys.stdin)["packages"] if p["name"] == "purrdf-text" and p["version"] == locked[0]]
if len(crates) != 1:
    refuse("cargo metadata does not report exactly one purrdf-text " + locked[0])
vcs_path = crates[0]["manifest_path"].rsplit("/", 1)[0] + "/.cargo_vcs_info.json"
try:
    with open(vcs_path, "rb") as f:
        published = json.load(f)["git"]["sha1"]
except (OSError, KeyError, ValueError) as error:
    refuse("cannot read the published revision from " + vcs_path + ": " + str(error))
if revision != published:
    refuse("registry git_revision " + revision + " is not the revision " + published
           + " that published purrdf-text " + locked[0] + " (" + vcs_path + "); update the registry")
print(repository, revision)
' "$checkout")

[[ $revision =~ ^[0-9a-f]{40}$ ]] || { echo "purrdf-lexicons: $revision is not a full 40-digit object name" >&2; exit 2; }
source_repository=$root/$repository
dest=$bindir/lexicons-$revision

staged=$(mktemp -d -- "$bindir/.lexicons.XXXXXX")
trap 'rm -rf -- "${staged:?}"' EXIT
env -u GIT_DIR -u GIT_WORK_TREE git -C "$source_repository" archive --format=tar "$revision" crates/text/lexicons \
    | tar -x --strip-components=3 -C "$staged"
[[ -d $staged/artifacts ]] || { echo "purrdf-lexicons: $revision in $source_repository has no crates/text/lexicons/artifacts directory" >&2; exit 1; }
count=$(find "$staged/artifacts" -maxdepth 1 -name '*.cbor' | wc -l)
((count == 5)) || { echo "purrdf-lexicons: $revision holds $count lexicon artifacts under crates/text/lexicons; expected 5" >&2; exit 1; }
chmod 0755 -- "$staged"
if [[ -d $dest ]]; then
    diff -r -q -- "$staged" "$dest" >/dev/null || { echo "purrdf-lexicons: $dest differs from $revision" >&2; exit 1; }
else
    mv -T -n -- "$staged" "$dest" 2>/dev/null || [[ -d $dest ]] || { echo "purrdf-lexicons: cannot place $dest" >&2; exit 1; }
fi
echo "$dest"
