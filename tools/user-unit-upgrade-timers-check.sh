#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Recorded check for deploy/user-unit-upgrade.sh (issue #175): the script must never start
# sparrow-backup.timer at the end of a switch, because that timer restarts only after a first online
# backup verifies (deploy/verify-backup.sh exits 0); and on its early-abort path it must start again
# only the timers that were active before it stopped them. It also covers the pre-upgrade backup
# (issue #195): without --no-backup the script exports SPARROW_PREFIX and passes sparrow-backup the
# arguments sparrow-backup.service passes.
#
# It runs the real script end to end in a scratch world: a fake HOME with kept images, links,
# configuration and a status-answering CLI, and stub systemctl, getcap, lsattr, ps and sleep first
# on PATH. The stub systemctl keeps timer state in files and logs every call, so the assertions read
# what the script asked of the user manager. Nothing here reaches a real unit, timer, store or
# daemon. It stays out of `make check` (it needs python3 and runs the whole script four times); run
# it after any change to the script's timer or backup handling.
#
# Usage: tools/user-unit-upgrade-timers-check.sh SCRATCH_DIR   (absolute; created if absent, never emptied)
# Prints "PASS" and exits 0, or names the failed assertion and exits 1.
set -euo pipefail
scratch=${1:?scratch dir}
[[ $scratch == /* ]] || { echo "user-unit-upgrade-timers-check: SCRATCH_DIR must be absolute" >&2; exit 2; }
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
script=$here/../deploy/user-unit-upgrade.sh
mkdir -p -- "$scratch"

fail() { echo "FAIL: $*" >&2; exit 1; }

# world NAME: builds $scratch/NAME with the stubs and prints nothing; the layout is the user unit's.
world() {
    local w=$scratch/$1
    [[ ! -e $w ]] || { echo "user-unit-upgrade-timers-check: $w already exists; use a fresh SCRATCH_DIR" >&2; exit 2; }
    local lib=$w/home/.local/libexec/sparrow bin=$w/home/.local/bin cfg=$w/home/.config/sparrow
    mkdir -p -- "$lib" "$bin" "$cfg" "$w/stubs" "$w/state" "$w/kept" "$w/backups" "$w/store" "$w/intake"
    local schema rev
    for pair in 68:aaa 71:bbb; do
        schema=${pair%%:*} rev=${pair##*:}
        printf '#!/bin/sh\nexit 0\n' >"$lib/sparrowd-schema$schema-$rev"
        # The CLI image answers status the way the daemon of its own schema would.
        cat >"$lib/sparrow-schema$schema-$rev" <<CLI
#!/bin/sh
printf '{"store_id":"s1","schema_version":$schema,"store_format":"f","live_records":0,"backup":{"phase":"idle"},"pressure":{"phase":"ready","failure":null},"durability":{"failure":null},"query_only":false,"consume_watches":[]}\n'
CLI
        chmod +x "$lib/sparrowd-schema$schema-$rev" "$lib/sparrow-schema$schema-$rev"
    done
    cp -- "$lib/sparrowd-schema71-bbb" "$lib/sparrow-schema71-bbb" "$w/kept/"
    ln -s -- sparrowd-schema68-aaa "$lib/sparrowd"
    ln -s -- "$lib/sparrow-schema68-aaa" "$bin/sparrow"
    : >"$w/intake/ingest.sock"
    cat >"$cfg/server.toml" <<TOML
data_dir = "$w/store"
socket_path = "$w/rpc.sock"
ingest_socket_path = "$w/intake/ingest.sock"
backup_root = "$w/backups"
TOML
    : >"$cfg/ship.toml"
    # Stubs: the script's host probes and the user manager.
    cat >"$w/stubs/getcap" <<'STUB'
#!/bin/sh
echo "$1 cap_net_bind_service=ep"
STUB
    cat >"$w/stubs/lsattr" <<'STUB'
#!/bin/sh
for a; do :; done
echo "---------------C------ $a"
STUB
    printf '#!/bin/sh\nexit 0\n' >"$w/stubs/ps"
    printf '#!/bin/sh\nexit 0\n' >"$w/stubs/sleep"
    cat >"$w/stubs/systemctl" <<'SYSTEMCTL'
#!/bin/bash
# State files: STATE/NAME.active exists when NAME is active. STUB_SHIP_BUSY=1 keeps
# sparrow-ship.service "activating" so the script's wait for the shipper fails.
[[ $1 == --user ]] && shift
echo "$*" >>"$STATE/calls"
case $1 in
    is-active)
        shift; [[ $1 == --quiet ]] && shift
        if [[ -e $STATE/$1.active ]]; then echo active; exit 0; fi
        echo inactive; exit 3 ;;
    show)
        case $3 in
            ActiveState)
                if [[ $5 == sparrow-ship.service && ${STUB_SHIP_BUSY:-0} == 1 ]]; then echo activating
                elif [[ -e $STATE/$5.active ]]; then echo active; else echo inactive; fi ;;
            LimitNOFILESoft) echo 65536 ;;
        esac ;;
    start) shift; for u; do touch "$STATE/$u.active"; done ;;
    stop) shift; for u; do rm -f -- "$STATE/$u.active"; done ;;
esac
exit 0
SYSTEMCTL
    chmod +x "$w/stubs/"*
    printf '%s\n' "$w"
}

script_args=(--no-backup 'timer check')
# run W [env...]: runs the script in world W with the given extra env; sets rc and out.
run() {
    local w=$1; shift
    set +e
    out=$(env -i PATH="$w/stubs:/usr/bin:/bin" HOME="$w/home" STATE="$w/state" \
        SPARROW_BACKUPS="$w/backups" SPARROW_BIN="$w/kept"\
        "$@" "$script" --from sparrowd-schema68-aaa --to sparrowd-schema71-bbb "${script_args[@]}" 2>&1)
    rc=$?
    set -e
}
starts() { grep -E '^start ' "$1/state/calls" || true; }

# 1. A successful switch starts sparrow-ship.timer and never sparrow-backup.timer, even though the
#    backup timer ran before; it prints the verify-then-start instruction.
w=$(world success)
touch "$w/state/sparrow-ship.timer.active" "$w/state/sparrow-backup.timer.active"
run "$w"
((rc == 0)) || { echo "$out" >&2; fail "success path exited $rc"; }
grep -qx 'start sparrow-ship.timer' <<<"$(starts "$w")" || fail "success path did not start sparrow-ship.timer"
! grep -q 'sparrow-backup.timer' <<<"$(starts "$w")" || fail "success path started sparrow-backup.timer: $(starts "$w")"
[[ -e $w/state/sparrow-ship.timer.active && ! -e $w/state/sparrow-backup.timer.active ]] \
    || fail "success path left the wrong timer states"
grep -q 'deploy/verify-backup.sh SET' <<<"$out" || fail "success path did not print the verify step"
grep -q 'systemctl --user start sparrow-backup.timer' <<<"$out" \
    || fail "success path did not print the verify-then-start instruction"
echo "ok   success: starts $(starts "$w" | tr '\n' ';')"

# 2. Abort with both timers active before: both come back, nothing else changed (no sparrow.service stop).
w=$(world abort-both)
touch "$w/state/sparrow-ship.timer.active" "$w/state/sparrow-backup.timer.active"
run "$w" STUB_SHIP_BUSY=1
((rc != 0)) || fail "abort path exited 0"
[[ -e $w/state/sparrow-ship.timer.active && -e $w/state/sparrow-backup.timer.active ]] \
    || fail "abort path did not restore both timers that were active"
! grep -q 'stop sparrow.service' "$w/state/calls" || fail "abort path stopped sparrow.service"
echo "ok   abort, both timers active before: starts $(starts "$w" | tr '\n' ';')"

# 3. Abort with the backup timer stopped before: only the shipper timer comes back.
w=$(world abort-ship-only)
touch "$w/state/sparrow-ship.timer.active"
run "$w" STUB_SHIP_BUSY=1
((rc != 0)) || fail "abort path exited 0"
[[ -e $w/state/sparrow-ship.timer.active && ! -e $w/state/sparrow-backup.timer.active ]] \
    || fail "abort path started a timer that was not active before"
! grep -q 'sparrow-backup.timer' <<<"$(starts "$w")" || fail "abort path started sparrow-backup.timer: $(starts "$w")"
echo "ok   abort, backup timer stopped before: starts $(starts "$w" | tr '\n' ';')"

# 4. Without --no-backup the script takes its pre-upgrade backup (issue #195): it exports
#    SPARROW_PREFIX and passes sparrow-backup the arguments sparrow-backup.service passes, all
#    derived from HOME. The stub client records them and writes the set the script then links.
w=$(world backup)
state=$w/home/.local/state/sparrow
mkdir -p -- "$state/intake" "$state/ship" "$state/ship-watches"
cat >"$w/home/.local/libexec/sparrow/sparrow-backup" <<STUB
#!/bin/sh
{ echo "prefix=\$SPARROW_PREFIX"; for a; do echo "arg=\$a"; done; } >"$w/client"
mkdir -p "$w/backups/sets/1"
printf 'image=sparrowd-schema68-aaa\nstore_id=s1\ntaken_utc=t\n' >"$w/backups/sets/1/SOURCE"
STUB
chmod +x "$w/home/.local/libexec/sparrow/sparrow-backup"
script_args=()
run "$w"
((rc == 0)) || { echo "$out" >&2; fail "backup path exited $rc"; }
expected=$(printf '%s\n' "prefix=$w/home/.local" "arg=$w/home/.config/sparrow/server.toml" "arg=$state/intake" \
    "arg=$w/home/.config/sparrow" "arg=$state/ship" "arg=$state/ship-watches")
[[ $(cat "$w/client") == "$expected" ]] || fail "sparrow-backup received: $(cat "$w/client")"
[[ -L $w/backups/pre-upgrade-sparrowd-schema68-aaa ]] || fail "backup path did not link the pre-upgrade set"
echo "ok   backup: sparrow-backup got the user service's arguments and SPARROW_PREFIX"

echo PASS
