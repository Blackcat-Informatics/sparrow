#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Recorded check for deploy/user-unit-upgrade.sh (issue #175): a successful switch starts
# sparrow-ship.timer again, and on its early-abort path the script starts again only the timer
# that was active before it stopped it, changing nothing else.
#
# It runs the real script end to end in a scratch world: a fake HOME with kept images, links,
# configuration and a status-answering CLI, and stub systemctl, getcap, lsattr, ps and sleep first
# on PATH. The stub systemctl keeps timer state in files and logs every call, so the assertions read
# what the script asked of the user manager. Nothing here reaches a real unit, timer, store or
# daemon. It stays out of `make check` (it needs python3 and runs the whole script four times); run
# it after any change to the script's timer handling.
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
    mkdir -p -- "$lib" "$bin" "$cfg" "$w/stubs" "$w/state" "$w/kept" "$w/store" "$w/intake"
    local schema rev
    for pair in 68:aaa 71:bbb; do
        schema=${pair%%:*} rev=${pair##*:}
        printf '#!/bin/sh\nexit 0\n' >"$lib/sparrowd-schema$schema-$rev"
        # The CLI image answers status the way the daemon of its own schema would.
        cat >"$lib/sparrow-schema$schema-$rev" <<CLI
#!/bin/sh
printf '{"store_id":"s1","schema_version":$schema,"store_format":"f","live_records":0,"pressure":{"phase":"ready","failure":null},"durability":{"failure":null},"query_only":false,"consume_watches":[]}\n'
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
TOML
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

# run W [env...]: runs the script in world W with the given extra env and the script arguments
# in extra; sets rc and out.
extra=()
run() {
    local w=$1; shift
    set +e
    out=$(env -i PATH="$w/stubs:/usr/bin:/bin" HOME="$w/home" STATE="$w/state" \
        SPARROW_BIN="$w/kept" \
        "$@" "$script" --from sparrowd-schema68-aaa --to sparrowd-schema71-bbb "${extra[@]}" 2>&1)
    rc=$?
    set -e
}
starts() { grep -E '^start ' "$1/state/calls" || true; }

# 1. A successful switch starts sparrow-ship.timer again.
w=$(world success)
touch "$w/state/sparrow-ship.timer.active"
run "$w"
((rc == 0)) || { echo "$out" >&2; fail "success path exited $rc"; }
grep -qx 'start sparrow-ship.timer' <<<"$(starts "$w")" || fail "success path did not start sparrow-ship.timer"
[[ -e $w/state/sparrow-ship.timer.active ]] || fail "success path left the shipper timer stopped"
echo "ok   success: starts $(starts "$w" | tr '\n' ';')"

# 2. Abort with the shipper timer active before: it comes back, nothing else changed (no
#    sparrow.service stop).
w=$(world abort-active)
touch "$w/state/sparrow-ship.timer.active"
run "$w" STUB_SHIP_BUSY=1
((rc != 0)) || fail "abort path exited 0"
[[ -e $w/state/sparrow-ship.timer.active ]] || fail "abort path did not restore the timer that was active"
! grep -q 'stop sparrow.service' "$w/state/calls" || fail "abort path stopped sparrow.service"
echo "ok   abort, timer active before: starts $(starts "$w" | tr '\n' ';')"

# 3. Abort with the shipper timer stopped before: it stays stopped.
w=$(world abort-stopped)
run "$w" STUB_SHIP_BUSY=1
((rc != 0)) || fail "abort path exited 0"
[[ ! -e $w/state/sparrow-ship.timer.active ]] || fail "abort path started a timer that was not active before"
! grep -q 'sparrow-ship.timer' <<<"$(starts "$w")" || fail "abort path started sparrow-ship.timer: $(starts "$w")"
echo "ok   abort, timer stopped before: starts none"

# 4. A configuration that still names a removed backup key is refused before anything is touched.
w=$(world stale-config)
printf 'backup_root = "/nowhere"\nbackup_keep = 3\n' >>"$w/home/.config/sparrow/server.toml"
touch "$w/state/sparrow-ship.timer.active"
run "$w"
((rc != 0)) || fail "stale-config path exited 0"
grep -q 'backup_root or backup_keep' <<<"$out" || fail "stale-config refusal did not name the keys: $out"
[[ ! -e $w/state/calls ]] || ! grep -Eq '^(stop|start) ' "$w/state/calls" || fail "stale-config path touched a unit"
[[ $(readlink "$w/home/.local/libexec/sparrow/sparrowd") == sparrowd-schema68-aaa ]] || fail "stale-config path moved a link"
echo "ok   stale backup keys: refused before any change"

# 5. --keep-copy copies the closed store after the daemon stops and before the links switch, and the
#    copy lists the store's files and sizes. The copy is a reflink copy here (the store directory has
#    copy-on-write off and the stub lsattr reports it off for every file), so the scratch directory
#    must be on a btrfs filesystem.
w=$(world keep-copy)
chattr +C -- "$w/store"
head -c 100000 /dev/zero >"$w/store/000001.sst"
touch "$w/state/sparrow-ship.timer.active"
extra=(--keep-copy "$w/store.before")
run "$w"
((rc == 0)) || { echo "$out" >&2; fail "keep-copy path exited $rc"; }
[[ $(stat -c %s "$w/store.before/000001.sst") == 100000 ]] || fail "keep-copy path did not copy the store's file"
[[ ! -e $w/store.before.partial ]] || fail "keep-copy path left its partial directory"
stopped=$(grep -n 'daemon stopped' <<<"$out" | cut -d: -f1)
copied=$(grep -n 'kept a reflink copy' <<<"$out" | cut -d: -f1)
switched=$(grep -n 'switch links' <<<"$out" | cut -d: -f1)
((stopped < copied && copied < switched)) || fail "the copy did not fall between the stop and the link switch: $out"
echo "ok   keep-copy: copied between the stop and the link switch"

# 6. A --keep-copy name that exists is refused before anything is touched.
w=$(world keep-copy-exists)
mkdir -- "$w/store.before"
touch "$w/state/sparrow-ship.timer.active"
extra=(--keep-copy "$w/store.before")
run "$w"
((rc != 0)) || fail "keep-copy-exists path exited 0"
grep -q 'already exists' <<<"$out" || fail "keep-copy refusal did not say the name exists: $out"
[[ ! -e $w/state/calls ]] || ! grep -Eq '^(stop|start) ' "$w/state/calls" || fail "keep-copy-exists path touched a unit"
extra=()
echo "ok   keep-copy name exists: refused before any change"

echo PASS
