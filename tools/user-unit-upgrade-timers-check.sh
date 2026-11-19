#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Recorded check for deploy/user-unit-upgrade.sh: a successful switch starts sparrow-ship.timer
# again (issue #175), and on its early-abort path the script starts again only the timer that was
# active before it stopped it, changing nothing else; --keep-copy copies the closed store between
# the stop and the link switch; --hold-after-copy (issue #403) pauses there, and proceed switches,
# abort or a hangup with dead output restarts the old image and restores the timer, and the option
# is refused without --keep-copy.
#
# It runs the real script end to end in a scratch world: a fake HOME with kept images, links,
# configuration and a status-answering CLI, and stub systemctl, getcap, lsattr, logger, ps and sleep first
# on PATH. The stub systemctl keeps timer state in files and logs every call, so the assertions read
# what the script asked of the user manager. Nothing here reaches a real unit, timer, store or
# daemon. It stays out of `make check` (it needs python3 and runs the whole script several times); run
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
    # shellcheck disable=SC2016 # the stub expands its own variables
    printf '#!/bin/sh\necho "$*" >>"$STATE/logger"\n' >"$w/stubs/logger"
    # STUB_PS: unset lists nothing, "other" lists unrelated processes, "daemon" lists a sparrowd,
    # "bigdaemon" lists a sparrowd first and then over 1 MiB of other names (a pipe to grep -q would
    # be cut by SIGPIPE under pipefail and read the daemon as gone),
    # "fail" exits 1 with no output the way ps does when it cannot read the process table.
    cat >"$w/stubs/ps" <<'STUB'
#!/bin/sh
# "-e -o comm=" prints names only; the listing form prints pid, name and arguments.
if [ "$*" = "-e -o comm=" ]; then
    case ${STUB_PS:-} in
        fail) exit 1 ;;
        other) echo systemd; echo bash ;;
        daemon) echo systemd; echo sparrowd ;;
        bigdaemon) echo sparrowd; yes systemd | head -c 1200000 ;;
    esac
    exit 0
fi
case ${STUB_PS:-} in
    fail) exit 1 ;;
    other) echo "    1 systemd systemd"; echo "  200 bash bash" ;;
    daemon|bigdaemon) echo "    1 systemd systemd"; echo "  300 sparrowd sparrowd --config x" ;;
esac
exit 0
STUB
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
    start) shift
        # STUB_HUP_ON_START=1: the login shell's SIGHUP reaches the script as it starts the service.
        [[ ${STUB_HUP_ON_START:-0} == 1 && $1 == sparrow.service ]] && kill -HUP "$PPID"
        for u; do touch "$STATE/$u.active"; done ;;
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
# wrap: a command prefix for the script's process (empty by default).
wrap=()
run() {
    local w=$1; shift
    set +e
    out=$("${wrap[@]}" env -i PATH="$w/stubs:/usr/bin:/bin" HOME="$w/home" STATE="$w/state" \
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

# 7. --hold-after-copy pauses after the copy and before the link switch; "proceed" continues to the
#    switch and the start, as without the hold. The answer comes through a named pipe.
w=$(world hold-proceed)
chattr +C -- "$w/store"
mkfifo -- "$w/answer"
touch "$w/state/sparrow-ship.timer.active"
extra=(--keep-copy "$w/store.before" --hold-after-copy --hold-answer "$w/answer")
echo proceed >"$w/answer" &
writer=$!
run "$w"
kill "$writer" 2>/dev/null || true
((rc == 0)) || { echo "$out" >&2; fail "hold-proceed path exited $rc"; }
copied=$(grep -n 'kept a reflink copy' <<<"$out" | cut -d: -f1)
held=$(grep -n 'HOLD:' <<<"$out" | head -1 | cut -d: -f1)
switched=$(grep -n 'switch links' <<<"$out" | cut -d: -f1)
((copied < held && held < switched)) || fail "the hold did not fall between the copy and the link switch: $out"
[[ $(readlink "$w/home/.local/libexec/sparrow/sparrowd") == sparrowd-schema71-bbb ]] || fail "hold-proceed path did not switch the link"
[[ -e $w/state/sparrow-ship.timer.active ]] || fail "hold-proceed path left the shipper timer stopped"
echo "ok   hold + proceed: held between the copy and the link switch, then switched"

# 8. "abort" at the hold starts the old image again on the unchanged store, switches nothing, restores
#    the timer and exits non-zero.
w=$(world hold-abort)
chattr +C -- "$w/store"
mkfifo -- "$w/answer"
touch "$w/state/sparrow-ship.timer.active"
extra=(--keep-copy "$w/store.before" --hold-after-copy --hold-answer "$w/answer")
echo abort >"$w/answer" &
writer=$!
run "$w"
kill "$writer" 2>/dev/null || true
((rc != 0)) || fail "hold-abort path exited 0"
[[ $(readlink "$w/home/.local/libexec/sparrow/sparrowd") == sparrowd-schema68-aaa ]] || fail "hold-abort path moved the daemon link"
[[ $(readlink "$w/home/.local/bin/sparrow") == "$w/home/.local/libexec/sparrow/sparrow-schema68-aaa" ]] || fail "hold-abort path moved the CLI link"
! grep -q 'switch links' <<<"$out" || fail "hold-abort path switched links"
[[ ! -e $w/home/.config/systemd/user/sparrow.service.d/limits.conf ]] || fail "hold-abort path installed the drop-in"
[[ -e $w/state/sparrow.service.active ]] || fail "hold-abort path left sparrow.service stopped"
[[ -e $w/state/sparrow-ship.timer.active ]] || fail "hold-abort path did not restore the timer"
[[ $(grep -En '^(stop|start) sparrow\.service' "$w/state/calls" | cut -d: -f2 | tr '\n' ';') == 'stop sparrow.service;start sparrow.service;' ]] \
    || fail "hold-abort path did not stop then start sparrow.service once"
[[ -d $w/store.before ]] || fail "hold-abort path lost the kept copy"
echo "ok   hold + abort: old image started again, nothing switched, exit $rc"

# 9. --hold-after-copy without --keep-copy is refused before anything is touched.
w=$(world hold-no-copy)
touch "$w/state/sparrow-ship.timer.active"
extra=(--hold-after-copy)
run "$w"
((rc != 0)) || fail "hold-no-copy path exited 0"
grep -q 'needs --keep-copy' <<<"$out" || fail "hold refusal did not name --keep-copy: $out"
[[ ! -e $w/state/calls ]] || fail "hold-no-copy path called systemctl"
extra=()
echo "ok   hold without keep-copy: refused before any change"

# 10. The terminal hangs up during the hold (SSH drop) and every write fails (the output pipe's reader has gone): the abort
#     still starts the old image first and restores the timer, swaps nothing, and leaves its
#     message in DIR.abort.log and the journal (logger) for the operator to find.
w=$(world hold-hangup)
chattr +C -- "$w/store"
mkfifo -- "$w/answer" "$w/output"
touch "$w/state/sparrow-ship.timer.active"
set +e
# The reader leaves once the hold has begun, so the script's later writes hit a closed pipe.
{ while IFS= read -r line; do [[ $line == *HOLD:* ]] && break; done; } <"$w/output" &
env -i PATH="$w/stubs:/usr/bin:/bin" HOME="$w/home" STATE="$w/state" SPARROW_BIN="$w/kept" \
    "$script" --from sparrowd-schema68-aaa --to sparrowd-schema71-bbb \
    --keep-copy "$w/store.before" --hold-after-copy --hold-answer "$w/answer" >"$w/output" 2>&1 &
pid=$!
# Opening the pipe for writing returns only when the script is reading it, that is, in the hold.
exec 4>"$w/answer"
kill -HUP "$pid"
wait "$pid"
rc=$?
exec 4>&-
set -e
((rc != 0)) || fail "hold-hangup path exited 0"
[[ -e $w/state/sparrow.service.active ]] || fail "hold-hangup path left sparrow.service stopped"
[[ -e $w/state/sparrow-ship.timer.active ]] || fail "hold-hangup path did not restore the timer"
[[ $(readlink "$w/home/.local/libexec/sparrow/sparrowd") == sparrowd-schema68-aaa ]] || fail "hold-hangup path moved the daemon link"
grep -q 'hold aborted (SIGHUP)' "$w/store.before.abort.log" || fail "hold-hangup path wrote no durable abort message"
grep -q 'hold aborted (SIGHUP)' "$w/state/logger" || fail "hold-hangup path wrote no journal message"
echo "ok   hold + hangup with dead output: old image restarted, message in ${w##*/}/store.before.abort.log"

# 11. An answer without a trailing newline is accepted.
w=$(world hold-nonewline)
chattr +C -- "$w/store"
mkfifo -- "$w/answer"
extra=(--keep-copy "$w/store.before" --hold-after-copy --hold-answer "$w/answer")
printf proceed >"$w/answer" &
writer=$!
run "$w"
kill "$writer" 2>/dev/null || true
extra=()
((rc == 0)) || { echo "$out" >&2; fail "hold answer without newline exited $rc"; }
echo "ok   hold answer without a trailing newline: accepted"

# 12. A hangup that arrives while the abort restores (the login shell's SIGHUP after the terminal
#     read ended) does not stop the restore: the service and timer come back and the abort is recorded.
w=$(world hold-hangup-in-abort)
chattr +C -- "$w/store"
mkfifo -- "$w/answer"
touch "$w/state/sparrow-ship.timer.active"
extra=(--keep-copy "$w/store.before" --hold-after-copy --hold-answer "$w/answer")
echo abort >"$w/answer" &
writer=$!
run "$w" STUB_HUP_ON_START=1
kill "$writer" 2>/dev/null || true
extra=()
((rc != 0)) || fail "hangup-in-abort path exited 0"
[[ -e $w/state/sparrow.service.active ]] || fail "hangup-in-abort path left sparrow.service stopped"
[[ -e $w/state/sparrow-ship.timer.active ]] || fail "hangup-in-abort path did not restore the timer"
grep -q 'hold aborted (operator)' "$w/store.before.abort.log" || fail "hangup-in-abort path wrote no abort record"
echo "ok   hangup during the abort: restore completed and recorded"

# 13. The "no sparrowd process remains" check fails closed (issue #410): a ps that errors stops the
#     script before the store copy (the daemon's state is unknown), a listed sparrowd still stops it
#     with the listing, and a list of other processes passes.
# "bigdaemon-notmp" runs the big list in a mount namespace whose /tmp and /var/tmp are 4 KiB tmpfs
# mounts and TMPDIR is unset, so a here-string over the pipe capacity cannot write bash's temp file
# (it would print "cannot create temp file" and read as no match); the match must do no I/O. The
# script refuses root, so the wrapper maps the operator's uid back in a nested user namespace.
for mode in fail daemon bigdaemon bigdaemon-notmp other; do
    w=$(world "ps-$mode")
    wrap=()
    if [[ $mode == *-notmp ]]; then
        printf '%s\n' '#!/bin/sh' 'mount -t tmpfs -o size=4k tmpfs /tmp && mount -t tmpfs -o size=4k tmpfs /var/tmp || exit 99' \
            "exec unshare -U --map-user=$(id -u) \"\$@\"" >"$w/notmp.sh"
        chmod +x "$w/notmp.sh"
        unshare -rm true || fail "ps $mode: unshare -rm (user and mount namespaces) is unavailable on this host"
        wrap=(unshare -rm "$w/notmp.sh")
    fi
    chattr +C -- "$w/store"
    touch "$w/state/sparrow-ship.timer.active"
    extra=(--keep-copy "$w/store.before")
    run "$w" STUB_PS=${mode%-notmp}
    extra=()
    wrap=()
    case $mode in
        other) ((rc == 0)) || { echo "$out" >&2; fail "ps listing other processes exited $rc"; } ;;
        *)
            ((rc != 0)) || fail "ps $mode: the script exited 0"
            grep -q 'stop sparrow.service' "$w/state/calls" || fail "ps $mode: the daemon was not stopped first"
            [[ ! -e $w/store.before && ! -e $w/store.before.partial ]] || fail "ps $mode: the store copy started"
            ! grep -q 'daemon stopped' <<<"$out" || fail "ps $mode: the script went on past the check"
            [[ $(readlink "$w/home/.local/libexec/sparrow/sparrowd") == sparrowd-schema68-aaa ]] || fail "ps $mode: a link moved"
            ;;
    esac
    case $mode in
        fail) grep -q 'whether a sparrowd still runs is unknown' <<<"$out" || fail "ps fail: no unknown-state message: $out" ;;
        daemon | bigdaemon | bigdaemon-notmp)
            grep -q 'a sparrowd process remains' <<<"$out" || fail "ps daemon: no refusal message: $out"
            grep -q '300 sparrowd' <<<"$out" || fail "ps daemon: no listing: $out" ;;
    esac
done
echo "ok   ps fails, lists sparrowd, lists others: stops before the copy, stops, passes"

echo PASS
