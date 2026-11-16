#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Post-deploy verification of a running Sparrow: the unit, the images, the socket's and store's
# permissions, the daemon's typed status (pressure, durability, store identity, watches), the
# unit's systemd-analyze exposure, AppArmor confinement and denials, and the timers.
# Exit 0: every check passed. 1: a check failed. 2: nothing failed, but a check needs root.
set -euo pipefail
umask 022

if (($# >= 1)) && [[ $1 == -h || $1 == --help ]]; then
    cat <<'USAGE'
Usage: deploy/check.sh [--store-id STORE_ID]

Verify the running service. With --store-id, also require that store identity (the move and
restore procedures record it beforehand). Run as root for the AppArmor and kernel-log checks.
USAGE
    exit 0
fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

EXPECTED_STORE=
case ${1:-} in
    --store-id) EXPECTED_STORE=${2:?--store-id needs a store identity}; shift 2 ;;
    '') ;;
    *) die "unknown argument $1 (see --help)" ;;
esac
(($# == 0)) || die "unexpected arguments: $*"
GROUP=${SPARROW_GROUP:-sparrow}
[[ $SPARROW_SCOPE == system ]] || GROUP=${SPARROW_GROUP:-$(id -gn)}
# The exposure the unit file scores today (deploy/evidence/systemd-analyze-security.md).
MAX_EXPOSURE=${SPARROW_MAX_EXPOSURE:-2.0}
FAILED=0 UNCHECKED=0

pass() { printf 'PASS  %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; FAILED=1; }
unchecked() { printf 'NOT CHECKED  %s\n' "$*"; UNCHECKED=1; }

if unit_active; then
    pass "$SPARROW_UNIT active, pid $("${SYSTEMCTL[@]}" show -p MainPID --value "$SPARROW_UNIT"), restarts $("${SYSTEMCTL[@]}" show -p NRestarts --value "$SPARROW_UNIT")"
else
    fail "$SPARROW_UNIT is $("${SYSTEMCTL[@]}" is-active "$SPARROW_UNIT" || true)"
fi

daemon=$(readlink -- "$LIBEXEC/sparrowd" || true)
if [[ -n $daemon && -x $LIBEXEC/$daemon ]]; then
    pass "daemon image $daemon (rollback: $(readlink -- "$LIBEXEC/sparrowd.previous" 2>/dev/null || echo none))"
else
    fail "no executable image behind $LIBEXEC/sparrowd"
fi

socket=$(config_value socket_path)
data=$(config_value data_dir)
if [[ -S $socket && $(stat -c '%a %G' -- "$socket") == "660 $GROUP" ]]; then
    pass "socket $socket is mode 0660, group $GROUP"
else
    fail "socket $socket: $(stat -c '%F %a %G' -- "$socket" 2>&1)"
fi
if [[ $(stat -c '%a %G' -- "$data") == "710 $GROUP" ]]; then
    pass "store $data is mode 0710, group $GROUP"
else
    fail "store $data: $(stat -c '%a %G' -- "$data" 2>&1)"
fi

if status=$(status_json 2>&1); then
    phase=$(status_field pressure phase <<<"$status")
    durability=$(status_field durability failure <<<"$status")
    store=$(status_field store_id <<<"$status")
    if [[ $phase == ready ]]; then pass "pressure phase ready"; else fail "pressure phase $phase"; fi
    if [[ $durability == None ]]; then pass "no durability failure"; else fail "durability failure: $durability"; fi
    if [[ -z $EXPECTED_STORE ]]; then
        schema=$(status_field schema_version <<<"$status")
        pass "store $store, schema $schema"
    elif [[ $store == "$EXPECTED_STORE" ]]; then
        pass "store identity $store as expected"
    else
        fail "store identity $store, expected $EXPECTED_STORE"
    fi
    # Captured first: a failure inside <(...) would read as no watches at all.
    watches=$(python3 -I -c 'import json,sys
for w in json.load(sys.stdin)["consume_watches"]:
    print(w["id"], w["enabled"], w["root"], w["pending_claims"], sep="\t")' <<<"$status")
    while IFS=$'\t' read -r id enabled root pending; do
        [[ -n $id ]] || continue
        if [[ $enabled == True && -d $root ]]; then
            pass "watch $id: $root, $pending pending claim(s)"
        else
            fail "watch $id: enabled=$enabled root=$root"
        fi
    done <<<"$watches"
else
    fail "status over $socket: $status"
fi

exposure=$(systemd-analyze security "${SYSTEMCTL[@]:1}" --no-pager "$SPARROW_UNIT" 2>/dev/null \
    | sed -n 's/.*Overall exposure level for .*: \([0-9.]*\).*/\1/p')
if [[ -z $exposure ]]; then
    fail "systemd-analyze security could not score $SPARROW_UNIT"
elif awk -v e="$exposure" -v m="$MAX_EXPOSURE" 'BEGIN { exit !(e <= m) }'; then
    pass "systemd-analyze security exposure $exposure (at most $MAX_EXPOSURE)"
else
    fail "systemd-analyze security exposure $exposure exceeds $MAX_EXPOSURE"
fi

if [[ $SPARROW_SCOPE == system ]]; then
    pid=$(systemctl show -p MainPID --value "$SPARROW_UNIT")
    if [[ $EUID -ne 0 ]]; then
        unchecked "AppArmor confinement and denials (run as root)"
    else
        label=$(cat "/proc/$pid/attr/apparmor/current" 2>/dev/null || cat "/proc/$pid/attr/current")
        if [[ $label == 'sparrow (enforce)' ]]; then pass "confined by $label"; else fail "AppArmor label: $label"; fi
        since=$(systemctl show -p ActiveEnterTimestamp --value "$SPARROW_UNIT")
        denials=$(journalctl -k --since "$since" --no-pager -o cat | grep -c 'apparmor="DENIED".*profile="sparrow"' || true)
        if ((denials == 0)); then pass "no AppArmor denials since $since"; else fail "$denials AppArmor denial(s) since $since (RECOVERY.md)"; fi
    fi
    for timer in sparrow-ship.timer; do
        if systemctl is-active --quiet "$timer"; then
            pass "$timer active, next $(systemctl show -p NextElapseUSecRealtime --value "$timer")"
        else
            fail "$timer is $(systemctl is-active "$timer" || true)"
        fi
    done
    slice=$(systemctl show -p Slice --value "$SPARROW_UNIT")
    if [[ $slice == lillith.slice ]]; then pass "in $slice"; else fail "in $slice, not the production slice"; fi
else
    unchecked "AppArmor, the timers and the slice (a user-scope test unit has none of them)"
fi

if ((FAILED)); then
    exit 1
elif ((UNCHECKED)); then
    exit 2
fi
