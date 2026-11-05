#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# `make seed-excludes-check`: every exclusion in .config/wt.toml must name something this host
# keeps, so the list cannot rot into patterns that guard nothing. Guards are the exceptions: names
# that must stay excluded even while nothing on this host carries them.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

# Credentials Sparrow never reads but seeding would copy, and the images make install-bin creates.
GUARDS=("/.env" "/.env.*" "/bin/")
LOCATIONS=("$HOME/.local/state/sparrow" "$HOME/.local/share/sparrow" "$HOME/.config/sparrow" /SPARROW)

mapfile -t patterns < <(sed -n '/^exclude = \[/,/^\]/s/^ *"\([^"]*\)",\{0,1\}$/\1/p' .config/wt.toml)
((${#patterns[@]})) || { echo "seed-excludes-check: no exclusions read from .config/wt.toml" >&2; exit 1; }

names=()
for location in "${LOCATIONS[@]}"; do
    [[ -r $location && -x $location ]] || continue
    while IFS= read -r -d '' entry; do
        names+=("$entry")
    done < <(find "$location" -mindepth 1 -maxdepth 2 -printf '%f%y\0' 2>/dev/null)
done

failed=0
for pattern in "${patterns[@]}"; do
    guard=0
    for g in "${GUARDS[@]}"; do [[ $pattern == "$g" ]] && guard=1; done
    if ((guard)); then
        echo "guard  $pattern"
        continue
    fi
    glob=${pattern%/} found=
    for entry in "${names[@]}"; do
        name=${entry%?} type=${entry: -1}
        [[ $pattern == */ && $type != d ]] && continue
        # shellcheck disable=SC2053 # the exclusion is a glob
        if [[ $name == $glob ]]; then found=$name; break; fi
    done
    if [[ -n $found ]]; then
        echo "found  $pattern ($found)"
    else
        echo "MISSING $pattern: nothing under ${LOCATIONS[*]} matches" >&2
        failed=1
    fi
done
exit "$failed"
