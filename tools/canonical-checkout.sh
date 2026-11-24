#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

set -euo pipefail

if (( $# != 1 )); then
    printf 'usage: tools/canonical-checkout.sh CHECKOUT\n' >&2
    exit 2
fi

if ! git_dir=$(git -C "$1" rev-parse --path-format=absolute --git-common-dir); then
    printf 'canonical-checkout: cannot find the Git common directory for %s\n' "$1" >&2
    exit 1
fi

realpath -e -- "$(dirname -- "$git_dir")"
