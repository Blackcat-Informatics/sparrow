#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Recorded check for deploy/libexec/katamari-manager-spool (issue #168): stopping a unit lands the
# manager's "Stopping ..." and "Stopped ..." lines in the spool, tagged "source":"systemd-manager",
# while the unit's own output stays out. It starts a throwaway transient USER unit with
# `systemd-run --user`, follows it with the script (KATAMARI_MANAGER_SCOPE=user) under its own
# cursor and a scratch spool, stops the unit, and reads the spool. It needs a systemd user manager
# and journal, so it is not part of `make check`.
#
# Usage: tools/katamari-manager-spool-check.sh SCRATCH_DIR   (absolute; created if absent, never emptied)
# Prints "PASS" and exits 0, or names the failed assertion and exits 1.
set -euo pipefail
scratch=${1:?scratch dir}
[[ $scratch == /* ]] || { echo "katamari-manager-spool-check: SCRATCH_DIR must be absolute" >&2; exit 2; }
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
script=$here/../deploy/libexec/katamari-manager-spool
mkdir -p -- "$scratch"
unit=sparrow-168-check-$$-$RANDOM.service
spool=$scratch/spool cursor=$scratch/manager.cursor
[[ ! -e $cursor ]] || { echo "katamari-manager-spool-check: $cursor already exists; use a fresh SCRATCH_DIR" >&2; exit 2; }
follower=''
cleanup() {
    if [[ -n $follower ]]; then kill -TERM -- "-$follower" 2>/dev/null || true; fi
    systemctl --user stop "$unit" 2>/dev/null || true
}
trap cleanup EXIT

systemd-run --user --quiet --unit="${unit%.service}" --description='sparrow 168 throwaway unit' \
    bash -c 'echo own-line-from-the-unit; exec sleep 600'
KATAMARI_MANAGER_SCOPE=user setsid "$script" "$unit" "$spool" "$cursor" 2>"$scratch/follower.err" &
follower=$!
sleep 2
systemctl --user stop "$unit"

# Wait for the "Stopped" line to be spooled, at most 30 seconds, checking once a second.
for _ in $(seq 30); do
    if cat -- "$spool"/katamari-manager-*.jsonl 2>/dev/null | grep -q '"message":"Stopped '; then break; fi
    sleep 1
done
files=("$spool"/katamari-manager-*.jsonl)
[[ -e ${files[0]} ]] || { echo "FAIL: no katamari-manager-*.jsonl in $spool"; exit 1; }
echo "--- spooled records"
cat -- "${files[@]}"
echo "---"
cat -- "${files[@]}" | grep -q '"message":"Stopping ' || { echo "FAIL: no Stopping line"; exit 1; }
cat -- "${files[@]}" | grep -q '"message":"Stopped ' || { echo "FAIL: no Stopped line"; exit 1; }
if cat -- "${files[@]}" | grep -q own-line-from-the-unit; then echo "FAIL: the unit's own line was spooled"; exit 1; fi
bad=$(cat -- "${files[@]}" | jq -c 'select(.source != "systemd-manager" or .unit != "'"$unit"'")' | wc -l)
[[ $bad == 0 ]] || { echo "FAIL: $bad records lack source=systemd-manager or the unit"; exit 1; }
# journalctl saves its cursor file as it ends, so end the follower and then read the cursor.
kill -TERM -- "-$follower" 2>/dev/null || true
wait "$follower" 2>/dev/null || true
follower=''
[[ -s $cursor ]] || { echo "FAIL: the cursor file is empty"; exit 1; }
echo PASS
