#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# tools/deploy-scripts-check.sh (issue #195; run after editing deploy/*.sh): cheap guards for deploy/*.sh. Every script parses; the
# user-unit upgrade exports SPARROW_PREFIX and passes sparrow-backup the arguments the installed user
# service passes (config, intake, config dir, ship state, ship-watches); verify-backup.sh removes the
# chunk file before each `bytes` read, which refuses to overwrite.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../deploy"
fail() { echo "deploy-scripts-check: $*" >&2; exit 1; }

for script in *.sh; do bash -n -- "$script" || fail "$script does not parse"; done

grep -qx 'export SPARROW_PREFIX' user-unit-upgrade.sh || fail "user-unit-upgrade.sh must export SPARROW_PREFIX"
grep -qF '"$LIBEXEC/sparrow-backup" "$SPARROW_CONFIG" \' user-unit-upgrade.sh || fail "sparrow-backup call not found"
grep -qF '"$SPARROW_INTAKE" "$CONFIG_DIR" "$SPARROW_STATE/ship" "$SPARROW_STATE/ship-watches"' user-unit-upgrade.sh \
    || fail "sparrow-backup arguments differ from the user service's"
grep -qx 'SPARROW_INTAKE=${SPARROW_INTAKE:-$SPARROW_STATE/intake}' user-unit-upgrade.sh \
    || fail "user-unit-upgrade.sh must default SPARROW_INTAKE to the user state tree"

line=$(grep -n '"\$cli" "\$SOCKET" bytes' verify-backup.sh | cut -d: -f1)
[[ -n $line ]] || fail "verify-backup.sh bytes read not found"
sed -n "$((line - 1))p" verify-backup.sh | grep -qF 'rm -f -- "$chunk"' || fail "verify-backup.sh must remove the chunk before each bytes read"
echo PASS
