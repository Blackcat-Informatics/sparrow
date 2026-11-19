#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Switch the operator's old user unit sparrow.service from one kept image pair to another.
#
# What an upgrade relies on: Sparrow backs up nothing but the archive age-out to GTS files, so
# this script takes no backup and requires none. The undo is the kept previous image (FROM stays in
# the libexec directory) and the link swap that points the daemon and CLI links back at it. That
# undo holds only while TO has not changed the store's format: the store's fence makes FROM refuse
# a store TO has upgraded. For a format-changing TO, --keep-copy DIR keeps a copy of the closed
# store, taken after the daemon stops and before TO opens it; the way back after TO has opened the
# store is that copy (docs/deployment.md, "Switch 1"), and without one the way out is to fix
# forward on TO. It serves until Sparrow issue #22 moves the store to the system unit.
# Ported from the operator's restart-bundle.sh; its predecessor restart-schema68.sh ran clean on
# the live host on 2026-11-07 (exit 0, 180 s down).
# Every check that can refuse runs before anything changes. Run it only when the restart is authorized.
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/user-unit-upgrade.sh --from DAEMON_IMAGE --to DAEMON_IMAGE [--keep-copy DIR [--hold-after-copy [--hold-answer FIFO]]]

Switch the user unit sparrow.service (systemctl --user) from the kept daemon image --from to
--to, both names under the libexec directory such as sparrowd-schema68-2aa56a9 and
sparrowd-schema69-3a64e08. Each CLI image is the daemon's name with sparrowd- replaced by
sparrow-; the schema numbers come from the names. Run as the operator, never as root.

What the upgrade relies on: no backup exists to restore from (Sparrow backs up nothing but the
archive age-out to GTS files). The undo is the kept previous image and the link swap. A
format-changing upgrade has no undo of that kind: once TO opens the store and upgrades its
format, FROM refuses it (the store fence). --keep-copy DIR keeps a copy of the store for that
case: after the daemon has stopped and before TO starts, the store is copied to the new
directory DIR (a reflink copy when the store and every file in it have copy-on-write off, a full
copy otherwise), the copy's file list is compared with the store's and DIR takes its name only
then. DIR must be a new name in an existing directory outside the store, and the filesystem
must hold the store's size plus 10 GiB. The way back after TO has opened the store is to put
that copy in the store's place (docs/deployment.md, "Switch 1"); without it the only way out is
forward on TO.

--hold-after-copy (needs --keep-copy) pauses the switch once the copy is complete and before the
links change or TO starts, so a rehearsal of the lift can run on a second copy under a scratch
unit while the daemon is down. The script prints the kept copy, the seconds since the daemon
stopped and the instructions, then waits, with no timeout (the daemon is down for the whole
hold; each prompt repeats the elapsed time). Answer "proceed" to switch the links and start TO as
usual, or "abort" to start the FROM image again on the unchanged store, start sparrow-ship.timer
if it was active, and exit non-zero. End of input on the terminal, Ctrl-C, SIGTERM and SIGHUP
abort the same way. The answer is read from the terminal; without one (stdin is not a terminal)
--hold-answer FIFO names an existing named pipe the answer is written to, for example
"echo proceed > FIFO". The script refuses the option, before anything changes, without
--keep-copy, and without a terminal or a FIFO.

Before anything changes it refuses unless: the daemon link names --from; all four images are
executable; the --to pair is byte-identical to the kept pair in SPARROW_BIN; the --to daemon
carries cap_net_bind_service (getcap: a human with root runs setcap, this script never does);
server.toml sets the ingest keys and names neither backup_root nor backup_keep (both keys
were removed; delete those two lines first, which the --from image also accepts); the store has copy-on-write off; the unit's drop-in directory
holds no .conf but an identical limits.conf (deploy/systemd/user/sparrow.service.d); and the
running daemon reports the --from schema. Then it stops sparrow-ship.timer and waits for the
shipper, stops the unit, installs limits.conf (LimitNOFILE=65536, the store refuses to open below
2048) with daemon-reload and checks the unit's LimitNOFILESoft, switches the links, starts the
unit, waits up to 600 s for the first status answer, checks the store identity and the --to
schema, prints the migrations, watches and ingest socket, and starts sparrow-ship.timer again. It
exits 0 without waiting for the online backfill, which continues in the running daemon; watch it
with "sparrow SOCKET status" (migrations) and journalctl --user -u sparrow.service. If the
shipper is still running when the timer has been stopped, the run starts the timer again if it
was active before and changes nothing else.

Environment (defaults are the user-unit layout):
  SPARROW_PREFIX   images in PREFIX/libexec/sparrow, CLI link PREFIX/bin/sparrow   ~/.local
  SPARROW_CONFIG   the daemon's server.toml, with ship.toml beside it; the store
                   (data_dir) and sockets are read from it       ~/.config/sparrow/server.toml
  SPARROW_BIN      the kept images the --to pair must match      <repository>/bin

Recovery depends on where it failed; the FAILED line names the unit and timer states.
- An aborted hold: the FROM image runs again on the unchanged store and the timer is restored;
  the exit status is 1 and the message says so. If the FROM image does not answer status, read
  journalctl --user -u sparrow.service.
- Before "stop timers": nothing stopped and the links name FROM. Fix the cause and rerun.
- From "stop timers" up to "switch links": the links still name FROM and the store is untouched.
  Start sparrow.service (if stopped) and sparrow-ship.timer. An installed limits.conf may stay:
  either image needs it. A failed --keep-copy leaves DIR.partial: inspect it, remove that one
  path, and rerun.
- At "switch links", before "start sparrow.service": check both links with readlink; one may
  name TO and the other FROM. Point both at FROM and recover as in the step above, or both at
  TO and start sparrow.service, then continue as below.
- After "start sparrow.service": unless the FAILED line shows the unit inactive or failed, the
  daemon runs TO with the timer stopped, and TO may have accepted ingest and watch work. Go
  forward first: wait for "sparrow SOCKET status" to answer (a slow open passes 600 s without
  failing), confirm its store_id and the TO schema, then start sparrow-ship.timer.
  Roll back by pointing the links at FROM only if TO did not change the store's format (the FROM
  image refuses a store TO upgraded: the store fence). After a format change there is no undo:
  fix forward on TO.
USAGE
}

FROM='' TO='' KEEP_COPY='' HOLD=0 HOLD_ANSWER=''
while (($#)); do
    case $1 in
        -h | --help) usage; exit 0 ;;
        --from | --to)
            (($# >= 2)) && [[ -n $2 ]] || { printf 'user-unit-upgrade.sh: %s needs a daemon image name\n' "$1" >&2; exit 2; }
            if [[ $1 == --from ]]; then FROM=$2; else TO=$2; fi
            shift 2
            ;;
        --keep-copy)
            (($# >= 2)) && [[ -n $2 ]] || { printf 'user-unit-upgrade.sh: --keep-copy needs a new directory name\n' >&2; exit 2; }
            KEEP_COPY=$2
            shift 2
            ;;
        --hold-after-copy) HOLD=1; shift ;;
        --hold-answer)
            (($# >= 2)) && [[ -n $2 ]] || { printf 'user-unit-upgrade.sh: --hold-answer needs a named pipe path\n' >&2; exit 2; }
            HOLD_ANSWER=$2
            shift 2
            ;;
        *) printf 'user-unit-upgrade.sh: unknown argument %s (see --help)\n' "$1" >&2; exit 2 ;;
    esac
done
[[ -n $FROM ]] || { printf 'user-unit-upgrade.sh: --from is required (see --help)\n' >&2; exit 2; }
[[ -n $TO ]] || { printf 'user-unit-upgrade.sh: --to is required (see --help)\n' >&2; exit 2; }
if ((HOLD)); then
    [[ -n $KEEP_COPY ]] || { printf 'user-unit-upgrade.sh: --hold-after-copy is defined after the store copy and needs --keep-copy DIR\n' >&2; exit 2; }
    if [[ -n $HOLD_ANSWER ]]; then
        [[ -p $HOLD_ANSWER ]] || { printf 'user-unit-upgrade.sh: --hold-answer %s is not an existing named pipe (mkfifo it first)\n' "$HOLD_ANSWER" >&2; exit 2; }
    else
        [[ -t 0 ]] || { printf 'user-unit-upgrade.sh: --hold-after-copy needs a terminal on stdin or --hold-answer FIFO\n' >&2; exit 2; }
    fi
elif [[ -n $HOLD_ANSWER ]]; then
    printf 'user-unit-upgrade.sh: --hold-answer needs --hold-after-copy\n' >&2; exit 2
fi

# The user-unit layout, set before lib.sh applies its system-unit defaults. lib.sh's own scope
# stays system: its unit helpers address SPARROW_UNIT, and it refuses user scope for
# sparrow.service, so this script drives the user manager itself (USER_SYSTEMCTL).
SPARROW_PREFIX=${SPARROW_PREFIX:-${HOME:?}/.local}
export SPARROW_PREFIX
SPARROW_CONFIG=${SPARROW_CONFIG:-${HOME:?}/.config/sparrow/server.toml}
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
USER_SYSTEMCTL=(systemctl --user)

((EUID != 0)) || die "run as the operator who owns the user unit, not as root"
for name in "$FROM" "$TO"; do
    [[ $name =~ ^sparrowd-schema[0-9]+-[A-Za-z0-9._-]+$ ]] \
        || die "$name is not a daemon image name sparrowd-schemaN-REV"
done
[[ $FROM != "$TO" ]] || die "--from and --to are both $FROM"
FROM_CLI=sparrow-${FROM#sparrowd-} TO_CLI=sparrow-${TO#sparrowd-}
FROM_SCHEMA=$(image_schema "$FROM") || exit 1
TO_SCHEMA=$(image_schema "$TO") || exit 1
# The switch gate: the first status answer within 10 minutes. The measured first open of schema 69
# on a copy of the live store under the unit's limits answered status in 117.2 s
# (measured 2026-11-07, image 3a64e08), so 600 s is the gate's whole
# budget, about five times the measurement, not a backfill bound: the open does not wait for the
# online migrations.
STATUS_SECONDS=600

stamp() { date -u +%FT%T.%3NZ; }
log() { printf '%s %s\n' "$(stamp)" "$*"; }
# A unit has settled when it is stopped; is-active's status alone counts "activating" as stopped.
settled() {
    local state
    state=$("${USER_SYSTEMCTL[@]}" show -p ActiveState --value "$1")
    [[ $state == inactive || $state == failed ]]
}

# Preconditions, all before anything changes.
current=$(readlink -- "$LIBEXEC/sparrowd" 2>/dev/null || true)
[[ $current == "$FROM" ]] || die "unexpected daemon link $LIBEXEC/sparrowd -> ${current:-none}, not $FROM"
refuse_schema_downgrade "$LIBEXEC/$TO"
for image in "$TO" "$TO_CLI" "$FROM" "$FROM_CLI"; do
    [[ -f $LIBEXEC/$image && -x $LIBEXEC/$image ]] || die "missing or not executable: $LIBEXEC/$image"
done
for image in "$TO" "$TO_CLI"; do
    cmp -s -- "$SPARROW_BIN/$image" "$LIBEXEC/$image" || die "$LIBEXEC/$image differs from $SPARROW_BIN/$image"
done
# The unit's descriptor limit (issue #148). The user unit sets none, so the daemon inherited the
# user manager's soft limit of 1024 and failed table reads with EMFILE; the store now refuses to
# open below 2048. The stopped window installs this drop-in. Any other drop-in in the unit's
# directory is refused, so nothing unreviewed changes the unit while the daemon is down.
LIMITS_SOURCE=$(dirname -- "${BASH_SOURCE[0]}")/systemd/user/sparrow.service.d/limits.conf
DROPIN_DIR=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/sparrow.service.d
LIMIT_NOFILE=65536
[[ -r $LIMITS_SOURCE ]] || die "cannot read $LIMITS_SOURCE"
for dropin in "$DROPIN_DIR"/*.conf; do
    [[ -e $dropin ]] || continue
    if [[ $dropin != "$DROPIN_DIR/limits.conf" ]] || ! cmp -s -- "$LIMITS_SOURCE" "$dropin"; then
        die "foreign drop-in $dropin; only $LIMITS_SOURCE is installed there. Move it aside or bring it into the repository first"
    fi
done
command -v getcap >/dev/null || die "getcap is missing; it checks the --to daemon's bind capability"
getcap -- "$LIBEXEC/$TO" 2>/dev/null | grep -q cap_net_bind_service \
    || die "$LIBEXEC/$TO lacks cap_net_bind_service; a human with root runs: setcap cap_net_bind_service=ep $LIBEXEC/$TO"
grep -q '^ingest_socket_path = ' "$SPARROW_CONFIG" || die "$SPARROW_CONFIG lacks the ingest keys"
# The online backup keys were removed (#330), and the server file refuses unknown keys, so the --to
# image would fail to load a file that still names them, after this script had stopped the daemon.
if grep -Eq '^[[:space:]]*(backup_root|backup_keep)[[:space:]]*=' "$SPARROW_CONFIG"; then
    die "$SPARROW_CONFIG still sets backup_root or backup_keep; the --to image refuses them. Delete those two lines (the --from image accepts the file without them) and run this again"
fi
STORE=$(config_value data_dir) || exit 1
SOCKET=$(config_value socket_path) || exit 1
INGEST_SOCKET=$(config_value ingest_socket_path) || exit 1
no_copy_on_write "$STORE" || die "$STORE is not no-COW (lsattr shows no C)"
# The store copy's method and size, checked while nothing has changed; the copy itself is made in
# the stopped window, below.
if [[ -n $KEEP_COPY ]]; then
    copy_plan=$(check_store_copy "$STORE" "$KEEP_COPY") || exit 1
    log "store copy planned at $KEEP_COPY: ${copy_plan%% *} copy of $STORE, ${copy_plan##* } bytes"
fi
# Start sparrow-ship.timer again if it was active before this script stopped it.
restore_ship_timer() {
    ((ship_timer_was_active == 0)) || "${USER_SYSTEMCTL[@]}" start sparrow-ship.timer
}
# Wait for the first status answer, not for pressure "ready". pressure.phase is the disk-budget
# maintenance state (sparrow-core storage/pressure.rs): it opens ready, or awaitingReclamation
# when a recovered marker says so, and only data-budget admission and pressure passes move it.
# The schema 69 migrations never set it, so it is not a migration signal, and gating on it would
# wait on budget maintenance, which a switch cannot hurry. It is reported by the switch instead.
# Sets out to the status answer.
wait_first_status() {
    local state deadline
    out=''
    deadline=$((SECONDS + STATUS_SECONDS))
    while :; do
        state=$("${USER_SYSTEMCTL[@]}" show -p ActiveState --value sparrow.service)
        if [[ $state == inactive || $state == failed ]]; then
            "${USER_SYSTEMCTL[@]}" status --no-pager --lines=20 sparrow.service >&2 || true
            die "sparrow.service is $state before its first status answer"
        fi
        if out=$("$CLI_LINK" "$SOCKET" status 2>/dev/null); then break; fi
        ((SECONDS < deadline)) || die "no status answer within ${STATUS_SECONDS}s, the switch gate; the daemon is left running"
        sleep 5
    done
}
elapsed_since_stop() { awk -v a="$EPOCHREALTIME" -v b="$T0" 'BEGIN{printf "%.1f", a-b}'; }
# The hold's abort: nothing has changed since the copy (links name FROM, the store is untouched),
# so the old image starts again, the timer comes back and the script exits non-zero.
abort_hold() {
    trap - INT TERM HUP
    log "hold aborted ($1) after $(elapsed_since_stop) s down: starting $FROM again on the unchanged store"
    "${USER_SYSTEMCTL[@]}" start sparrow.service
    wait_first_status
    restore_ship_timer
    log "the daemon runs $FROM again; the links name $FROM and the store is unchanged; the kept copy $KEEP_COPY remains"
    die "switch aborted at the hold; nothing was switched"
}
# Wait for proceed or abort, with no timeout: the daemon is down for the whole hold.
hold_after_copy() {
    local answer=''
    trap 'abort_hold SIGINT' INT
    trap 'abort_hold SIGTERM' TERM
    trap 'abort_hold SIGHUP' HUP
    while :; do
        printf '%s HOLD: sparrow.service has been down %s s. The closed store is kept at %s.\n' \
            "$(stamp)" "$(elapsed_since_stop)" "$KEEP_COPY"
        printf 'Rehearse the lift on a second copy now, then answer: "proceed" switches the links and starts %s; "abort" starts %s again on the unchanged store.\n' "$TO" "$FROM"
        if [[ -n $HOLD_ANSWER ]]; then
            read -r answer <"$HOLD_ANSWER" || answer=''
            [[ -n $answer ]] || continue
        else
            read -r -p 'proceed or abort> ' answer || { printf '\n'; abort_hold "end of input"; }
        fi
        case $answer in
            proceed) break ;;
            abort) abort_hold "operator" ;;
            *) printf 'not an answer: %s\n' "$answer" ;;
        esac
    done
    trap - INT TERM HUP
    log "hold released after $(elapsed_since_stop) s down: proceeding"
}
# After the preconditions, a failure says what state it left (the recovery note is in --help).
report_failure() {
    local rc=$?
    ((rc == 0)) && return
    printf '%s FAILED (exit %s). sparrow.service is %s; timer: ship %s. Read the recovery note: deploy/user-unit-upgrade.sh --help\n' \
        "$(stamp)" "$rc" "$("${USER_SYSTEMCTL[@]}" is-active sparrow.service)" \
        "$("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer)" >&2
}
trap report_failure EXIT
before=$("$CLI_LINK" "$SOCKET" status) || die "the running daemon does not answer status on $SOCKET"
STORE_ID=$(status_field store_id <<<"$before")
FORMAT=$(status_field store_format <<<"$before")
schema=$(status_field schema_version <<<"$before")
[[ $schema == "$FROM_SCHEMA" ]] || die "the running daemon reports schema $schema, not $FROM's $FROM_SCHEMA"
log "store_id before: $STORE_ID, format $FORMAT, schema $schema"
# Whether the shipper timer ran before this script stopped it: the abort path restores it.
ship_timer_was_active=0
"${USER_SYSTEMCTL[@]}" is-active --quiet sparrow-ship.timer && ship_timer_was_active=1
log "stop timers"
"${USER_SYSTEMCTL[@]}" stop sparrow-ship.timer
for _ in $(seq 360); do
    settled sparrow-ship.service && break
    sleep 5
done
if ! settled sparrow-ship.service; then
    restore_ship_timer
    die "the shipper is still running; sparrow-ship.timer is started again if it was active before, nothing else changed"
fi
T0=$EPOCHREALTIME
log "stop sparrow.service"
"${USER_SYSTEMCTL[@]}" stop sparrow.service
settled sparrow.service || die "sparrow.service has not settled"
if [[ -n $(ps -C sparrowd -o pid=) ]]; then
    ps -C sparrowd -o pid,args >&2
    die "a sparrowd process remains"
fi
log "daemon stopped"
if [[ -n $KEEP_COPY ]]; then
    log "copy the closed store to $KEEP_COPY"
    keep_store_copy "$STORE" "$KEEP_COPY"
fi
if ((HOLD)); then
    hold_after_copy
fi

log "install $DROPIN_DIR/limits.conf (LimitNOFILE=$LIMIT_NOFILE)"
install -D -m 0644 -- "$LIMITS_SOURCE" "$DROPIN_DIR/limits.conf"
"${USER_SYSTEMCTL[@]}" daemon-reload
soft=$("${USER_SYSTEMCTL[@]}" show -p LimitNOFILESoft --value sparrow.service)
[[ $soft == "$LIMIT_NOFILE" ]] \
    || die "sparrow.service has LimitNOFILESoft=$soft after daemon-reload, not $LIMIT_NOFILE; another unit setting overrides $DROPIN_DIR/limits.conf"

log "switch links"
swap_link "$LIBEXEC/sparrowd" "$TO"
swap_link "$CLI_LINK" "$LIBEXEC/$TO_CLI"
log "start sparrow.service ($TO)"
"${USER_SYSTEMCTL[@]}" start sparrow.service

wait_first_status
log "status answers; downtime $(elapsed_since_stop) s"
python3 -I -c 'import json, sys
v = json.load(sys.stdin)
# counts=pending (schema 70 and newer, while the store counters seed): live_records is exact
# only through the seeding watermark, not a total. An older image reports no counts; exact.
print("store_id=%s schema=%s format=%s live_records=%s counts=%s pressure=%s pressure_failure=%s durability_failure=%s query_only=%s"
      % (v["store_id"], v["schema_version"], v["store_format"], v["live_records"], v.get("counts", "exact"),
         v["pressure"]["phase"], v["pressure"]["failure"], v["durability"]["failure"], v["query_only"]))' <<<"$out"
now=$(status_field store_id <<<"$out")
[[ $now == "$STORE_ID" ]] || die "STORE ID CHANGED from $STORE_ID to $now; stop and read the recovery note"
schema=$(status_field schema_version <<<"$out")
[[ $schema == "$TO_SCHEMA" ]] || die "the daemon reports schema $schema, not $TO's $TO_SCHEMA"
python3 -I -c 'import json, sys
v = json.load(sys.stdin)
rows = v.get("migrations")
if rows is None:
    print("migrations: status reports none")
for r in rows or []:
    print("migration name=%s done=%s total=%s failure=%s" % (r["name"], r["done"], r["total"], r["failure"]))
for w in v["consume_watches"]:
    print("watch id=%s coverage=%s pending_claims=%s failure=%s"
          % (w.get("id"), w.get("coverage"), w.get("pending_claims"), w.get("failure")))' <<<"$out"
stat -c 'ingest socket %n %a %U:%G' -- "$INGEST_SOCKET"
"${USER_SYSTEMCTL[@]}" start sparrow-ship.timer
log "timer: ship $("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer)"
log "switched to $TO. Any online backfill continues in the running daemon; watch it with:"
printf '  %s %s status   (migrations: done/total, failure)\n' "$CLI_LINK" "$SOCKET"
printf "  journalctl --user -u sparrow.service -g 'sparrowd: (migration|online migration failed)'\n"
