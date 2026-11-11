#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Materialise the PurRDF text lexicons of one pinned revision at DEST. The registry crates exclude
# them (`exclude = ["lexicons/**"]`), so they come from the PurRDF repository at exactly the
# `git_revision` that registry/compatibility.toml records, archived with `git archive`, never from
# a working tree. An existing DEST must hold the same bytes; it is never replaced.
#
#   tools/purrdf-lexicons.sh REPOSITORY REVISION DEST
set -euo pipefail

(($# == 3)) || { echo "usage: purrdf-lexicons.sh REPOSITORY REVISION DEST" >&2; exit 2; }
repository=$1 revision=$2 dest=$3
[[ $revision =~ ^[0-9a-f]{40}$ ]] || { echo "purrdf-lexicons: $revision is not a full 40-digit object name" >&2; exit 2; }

parent=$(dirname -- "$dest")
mkdir -p -- "$parent"
staged=$(mktemp -d -- "$parent/.lexicons.XXXXXX")
trap 'rm -rf -- "${staged:?}"' EXIT
env -u GIT_DIR -u GIT_WORK_TREE git -C "$repository" archive --format=tar "$revision" crates/text/lexicons \
    | tar -x --strip-components=3 -C "$staged"
count=$(find "$staged/artifacts" -maxdepth 1 -name '*.cbor' 2>/dev/null | wc -l)
((count == 5)) || { echo "purrdf-lexicons: $revision holds $count lexicon artifacts under crates/text/lexicons; expected 5" >&2; exit 1; }
chmod 0755 -- "$staged"
if [[ -d $dest ]]; then
    diff -r -q -- "$staged" "$dest" >/dev/null || { echo "purrdf-lexicons: $dest differs from $revision" >&2; exit 1; }
    echo "purrdf-lexicons: kept $dest"
else
    mv -T -n -- "$staged" "$dest" 2>/dev/null || [[ -d $dest ]] || { echo "purrdf-lexicons: cannot place $dest" >&2; exit 1; }
    echo "purrdf-lexicons: $dest"
fi
