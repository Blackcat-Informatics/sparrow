#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Checks the spool and shipper invariant: every shipper [[source]] that reads unit-spool files has
# settle_seconds at least SPOOL_SETTLE_MARGIN_SECONDS greater than the spool's SPOOL_QUIET_SECONDS, so a file is shipped only after the
# spool has closed it. Usage: spool-settle.sh [SHIP_CONFIG]. With a config, checks that file and exits 1 when a
# source breaks the invariant. Without one, checks the fixtures in spool-settle/: good and unrelated pass, equal,
# near and short are refused. Prints PASS, or names the failure and exits 1.
set -euo pipefail
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
script=$here/../libexec/sparrow-unit-spool
quiet=$(sed -n 's/^SPOOL_QUIET_SECONDS=//p' "$script")
margin=$(sed -n 's/^SPOOL_SETTLE_MARGIN_SECONDS=//p' "$script")
[[ $quiet =~ ^[0-9]+$ && $margin =~ ^[0-9]+$ ]] || { printf 'spool-settle: FAIL: no spool quiet period or settle margin\n' >&2; exit 1; }
minimum=$((quiet + margin))

# Prints one line per unit-spool source that does not settle after the quiet period; exits 1 if any.
check() {
    awk -v minimum="$minimum" -v quiet="$quiet" -v margin="$margin" '
        function flush() {
            if (in_source && pattern ~ /unit-spool/ && (settle == "" || settle + 0 < minimum + 0)) {
                printf "%s: source %s settles at %s seconds, under the minimum %s (quiet %s plus margin %s)\n", FILENAME, name, (settle == "" ? "no" : settle), minimum, quiet, margin
                bad = 1
            }
            in_source = 0
        }
        /^\[\[source\]\]/ { flush(); in_source = 1; name = ""; pattern = ""; settle = ""; next }
        /^\[/ { flush(); next }
        in_source && /^name[ \t]*=/ { name = $0; sub(/^[^=]*=[ \t]*"?/, "", name); sub(/"?[ \t]*(#.*)?$/, "", name) }
        in_source && /^pattern[ \t]*=/ { pattern = $0 }
        in_source && /^settle_seconds[ \t]*=/ { settle = $0; sub(/^[^=]*=[ \t]*/, "", settle); sub(/[ \t#].*$/, "", settle) }
        END { flush(); exit bad }
    ' "$1"
}

if [[ $# -ge 1 ]]; then
    check "$1" || { printf 'spool-settle: FAIL: %s breaks the spool invariant\n' "$1" >&2; exit 1; }
    printf 'PASS\n'
    exit 0
fi

fixtures=$here/spool-settle
fail() { printf 'spool-settle: FAIL: %s\n' "$*" >&2; exit 1; }
check "$fixtures/good.toml" || fail "a source settling at 900 seconds was refused"
check "$fixtures/unrelated.toml" || fail "a source that does not read unit-spool files was refused"
if check "$fixtures/equal.toml" >/dev/null; then fail "a source settling at exactly the quiet period was accepted"; fi
if check "$fixtures/short.toml" >/dev/null; then fail "a source settling before the quiet period was accepted"; fi
if check "$fixtures/near.toml" >/dev/null; then fail "a source one second under the minimum was accepted"; fi
printf 'PASS\n'
