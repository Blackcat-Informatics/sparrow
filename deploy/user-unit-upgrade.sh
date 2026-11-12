#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Switch the operator's old user unit sparrow.service from one kept image pair to another, with
# the named pre-upgrade backup taken online through the running daemon before anything stops
# (Patrick, 2026-11-08: no backup may stop the server). --no-backup REASON switches without one
# when the running daemon has no online backup (coordination D147: the schema 68 to 71 switch). It serves until
# Sparrow issue #22 moves the store to the system unit, whose scripts (restart.sh,
# pre-upgrade-backup.sh) refuse the user unit. Ported from the operator's restart-bundle.sh; its
# predecessor restart-schema68.sh ran clean on the live host on 2026-11-07 (exit 0, 180 s down).
# Every check that can refuse runs before anything changes. Run it only on coordination's "go".
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/user-unit-upgrade.sh --from DAEMON_IMAGE --to DAEMON_IMAGE [--no-backup REASON]

Switch the user unit sparrow.service (systemctl --user) from the kept daemon image --from to
--to, both names under the libexec directory such as sparrowd-schema68-2aa56a9 and
sparrowd-schema69-3a64e08. Each CLI image is the daemon's name with sparrowd- replaced by
sparrow-; the schema numbers come from the names. Run as the operator, never as root.

Before anything changes it refuses unless: BACKUPS/pre-upgrade-FROM is absent; the daemon link
names --from; all four images are executable; the --to pair is byte-identical to the kept pair
in SPARROW_BIN; the --to daemon carries cap_net_bind_service (getcap: a human with root runs
setcap, this script never does); server.toml sets the ingest keys and backup_root equals
SPARROW_BACKUPS (with --no-backup too, so TO starts with its online backup; add the keys
immediately before the run, since a FROM image without them refuses to start with them, and
remove them before any rollback to such an image); the store has copy-on-write off; the unit's drop-in directory holds no .conf
but an identical limits.conf (deploy/systemd/user/sparrow.service.d); no nightly backup is
running; the running daemon reports the --from schema; and, without --no-backup, the running
daemon reports an online backup configured (its status has a backup state other than
unconfigured), and the intake, ship and ship-watches directories exist. Then, with the daemon serving, it takes the named pre-upgrade backup online
(SPARROW_PREFIX/libexec/sparrow/sparrow-backup: the daemon copies its store into backup_root and
the script records the set beside it) and points BACKUPS/pre-upgrade-FROM at that set. Only then
does it stop both timers and wait for the shipper, stop the unit, install limits.conf
(LimitNOFILE=65536, the store refuses to open below 2048) with daemon-reload and check the unit's
LimitNOFILESoft,
switches the links, starts the unit, waits up to
600 s for the first status answer, checks the store identity and the --to schema, prints the
migrations, watches and ingest socket, and starts sparrow-ship.timer again. It never starts
sparrow-backup.timer: the installed sparrow-backup.service may still be the old closed-store form,
and the timer restarts only after a first online backup verifies. It stays stopped, and the run
ends by printing the steps: take the first online backup, run deploy/verify-backup.sh SET, and only
after it exits 0 run systemctl --user start sparrow-backup.timer. It exits 0 without waiting
for the online backfill, which continues in the running daemon; watch it with
"sparrow SOCKET status" (migrations) and journalctl --user -u sparrow.service. If the shipper or
the backup is still running when the timers have been stopped, the run starts again only the
timers that were active before it stopped them (so the backup timer only if it was running) and
changes nothing else.

--no-backup REASON  switch without a pre-upgrade backup, for a running daemon that has no online
                    backup (an image older than the backup operation, such as schema 68). REASON
                    is required and must not be empty; the run logs it prominently. A stopped
                    copy is never taken instead: no backup may stop the server. Without a backup
                    a rollback can only go forward: there is no store to restore FROM's format
                    from, so fix forward on TO. Take the first online backup as soon as status
                    answers after the start: "sparrow SOCKET backup" and watch status, or run
                    the sparrow-backup client (the nightly sparrow-backup.timer runs it too, but
                    this script leaves that timer stopped until a backup verifies).

Environment (defaults are the user-unit layout):
  SPARROW_PREFIX   images in PREFIX/libexec/sparrow, CLI link PREFIX/bin/sparrow   ~/.local
  SPARROW_CONFIG   the daemon's server.toml, with ship.toml beside it; the store
                   (data_dir) and sockets are read from it       ~/.config/sparrow/server.toml
  SPARROW_BACKUPS  the online backup root (server.toml backup_root), on the NAS
                                                                 /net/synology/archives/sparrow
  SPARROW_STATE    the state tree holding ship and ship-watches  ~/.local/state/sparrow
  SPARROW_INTAKE   the intake tree the backup set keeps          ~/.local/state/sparrow/intake
  SPARROW_BIN      the kept images the --to pair must match      <repository>/bin

Recovery depends on where it failed; the FAILED line names the unit and timer states.
- Before "stop timers": nothing stopped and the links name FROM. A completed
  BACKUPS/pre-upgrade-FROM link must be moved aside before a rerun. Fix the cause and rerun.
- From "stop timers" up to "switch links": the links still name FROM and the store is untouched.
  Start sparrow.service (if stopped) and sparrow-ship.timer; the backup timer only if it was
  running before the run and the online sparrow-backup.service is installed. Move BACKUPS/pre-upgrade-FROM aside before
  a rerun. An installed limits.conf may stay: either image needs it.
- At "switch links", before "start sparrow.service": check both links with readlink; one may
  name TO and the other FROM. Point both at FROM and recover as in the step above, or both at
  TO and start sparrow.service, then continue as below.
- After "start sparrow.service": unless the FAILED line shows the unit inactive or failed, the
  daemon runs TO with both timers stopped, and TO may have accepted ingest and watch work.
  Go forward first: wait for "sparrow SOCKET status" to answer (a slow open passes 600 s
  without failing), confirm its store_id and the TO schema, then start sparrow-ship.timer. Start
  sparrow-backup.timer only after a first online backup verifies (deploy/verify-backup.sh SET
  exits 0).
  Roll back only if TO cannot serve, and only with a pre-upgrade backup (after --no-backup there
  is none: go forward). Stop sparrow.service; point the daemon link at FROM and the CLI link at
  the FROM CLI (an image restored from the set's images/ needs setcap cap_net_bind_service=ep,
  run by a human with root); move the store aside and restore the backup into an empty chattr +C
  directory with an image that has --restore-backup (TO does): TO --restore-backup BACKUPS ID
  STORE, where ID is backup_id in BACKUPS/pre-upgrade-FROM/SOURCE. That drops everything
  accepted after the backup started; then start sparrow.service and sparrow-ship.timer. Start
  sparrow-backup.timer only after a first online backup of the restored store verifies
  (deploy/verify-backup.sh SET exits 0).
- Claims after a rollback or restore (docs/deployment.md, "Claims after a rollback"). Keep the
  set-aside TO store: it records every claim TO made. Before starting sparrow.service on the
  restored store, open the restored store with the FROM image and the set-aside store with the
  TO image, each query-only on its own scratch configuration and socket, then run
  deploy/reconcile-inflight.sh inflight --restored FROM_CLI SOCK1 --newer TO_CLI SOCK2.
  It lists each consume root's .sparrow-inflight/ and moves every entry whose claim the
  restored catalog does not know back to its root under its original batch name, by a hard
  link under that name and an unlink of the entry (same inode, bytes unchanged). Without
  this the daemon refuses to start with
  "unrecognized private claim entry" (or "private claim directory exceeds its bounded journal
  registry"). Stop the restored query-only daemon, start sparrow.service, then run
  deploy/reconcile-inflight.sh missing --restored FROM_CLI SOCKET --newer TO_CLI SOCK2 for
  claims TO published after the backup: the restored store still journals them, each stays
  pending with the reason "unpublished claimed source is missing" in sparrow SOCKET watch
  claims (schema 71 and newer) until the claim is retired. It reads each one's bytes back
  from the set-aside store, checks them against their catalog digest, re-ships them into
  the root as NAME.reshipped.EXT and retires the claim with sparrow SOCKET watch
  retire-claim WATCH_ID CLAIM_ID "REASON" (schema 70 and newer). A retired claim leaves
  pending_claims, status and watch list show it as retired-unrecoverable, and the daemon
  logs one line. It exits 4 while a watch holds a pending claim it did not reconcile:
  inspect watch claims, rerun only for a claim just attempted, and decide on any other
  kind. A FROM image older than schema 70 cannot retire (exit 3 when nothing else is unreconciled): the bytes are
  re-shipped and the claim stays pending until a schema 70 image retires it. A claim
  whose private source exists or whose publication is recorded is refused.
USAGE
}

FROM='' TO='' NO_BACKUP=0 NO_BACKUP_REASON=''
while (($#)); do
    case $1 in
        -h | --help) usage; exit 0 ;;
        --no-backup)
            (($# >= 2)) && [[ -n ${2//[[:space:]]/} ]] || { printf 'user-unit-upgrade.sh: --no-backup needs a nonempty REASON\n' >&2; exit 2; }
            NO_BACKUP=1 NO_BACKUP_REASON=$2
            shift 2
            ;;
        --from | --to)
            (($# >= 2)) && [[ -n $2 ]] || { printf 'user-unit-upgrade.sh: %s needs a daemon image name\n' "$1" >&2; exit 2; }
            if [[ $1 == --from ]]; then FROM=$2; else TO=$2; fi
            shift 2
            ;;
        *) printf 'user-unit-upgrade.sh: unknown argument %s (see --help)\n' "$1" >&2; exit 2 ;;
    esac
done
[[ -n $FROM ]] || { printf 'user-unit-upgrade.sh: --from is required (see --help)\n' >&2; exit 2; }
[[ -n $TO ]] || { printf 'user-unit-upgrade.sh: --to is required (see --help)\n' >&2; exit 2; }

# The user-unit layout, set before lib.sh applies its system-unit defaults. lib.sh's own scope
# stays system: its unit helpers address SPARROW_UNIT, and it refuses user scope for
# sparrow.service, so this script drives the user manager itself (USER_SYSTEMCTL).
SPARROW_PREFIX=${SPARROW_PREFIX:-${HOME:?}/.local}
export SPARROW_PREFIX
# The user unit's state tree, as sparrow-backup.service passes it (%h/.local/state/sparrow/...).
SPARROW_STATE=${SPARROW_STATE:-${HOME:?}/.local/state/sparrow}
SPARROW_INTAKE=${SPARROW_INTAKE:-$SPARROW_STATE/intake}
SPARROW_CONFIG=${SPARROW_CONFIG:-${HOME:?}/.config/sparrow/server.toml}
# The user-unit pre-upgrade backup prints its own progress; lib.sh supplies the backup root.
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
BACKUP=$SPARROW_BACKUPS/pre-upgrade-$FROM
CONFIG_DIR=$(dirname -- "$SPARROW_CONFIG")
# The switch gate: the first status answer within 10 minutes. The measured first open of schema 69
# on a copy of the live store under the unit's limits answered status in 117.2 s
# (verify-backup.sh --measure-migration, 2026-11-07, image 3a64e08), so 600 s is the gate's whole
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
[[ ! -e $BACKUP && ! -L $BACKUP ]] || die "pre-upgrade backup $BACKUP exists; move it aside first"
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
STORE=$(config_value data_dir) || exit 1
SOCKET=$(config_value socket_path) || exit 1
INGEST_SOCKET=$(config_value ingest_socket_path) || exit 1
[[ -r $CONFIG_DIR/ship.toml ]] || die "cannot read $CONFIG_DIR/ship.toml; the backup keeps it"
no_copy_on_write "$STORE" || die "$STORE is not no-COW (lsattr shows no C)"
# TO must start with its online backup configured, or its first backup would need another restart.
# With --no-backup, FROM predates the keys and a FROM restart would refuse them, so they are added
# immediately before this run (the running FROM daemon does not reread its configuration) and
# removed again before any rollback to FROM.
root=$(config_value backup_root 2>/dev/null) || die "$SPARROW_CONFIG sets no backup_root, so $TO \
would start without its online backup. Add backup_root = \"$SPARROW_BACKUPS\" and backup_keep \
immediately before this switch (README.md, \"Interim user unit\")"
[[ $root == "$SPARROW_BACKUPS" ]] || die "$SPARROW_CONFIG backs up into $root, not \
SPARROW_BACKUPS=$SPARROW_BACKUPS"
((NO_BACKUP)) || [[ -x $LIBEXEC/sparrow-backup ]] || die "missing or not executable: $LIBEXEC/sparrow-backup"
# After the preconditions, a failure says what state it left (the recovery note is in --help).
report_failure() {
    local rc=$?
    ((rc == 0)) && return
    printf '%s FAILED (exit %s). sparrow.service is %s; timers: ship %s, backup %s. Read the recovery note: deploy/user-unit-upgrade.sh --help\n' \
        "$(stamp)" "$rc" "$("${USER_SYSTEMCTL[@]}" is-active sparrow.service)" \
        "$("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer)" \
        "$("${USER_SYSTEMCTL[@]}" is-active sparrow-backup.timer)" >&2
}
trap report_failure EXIT
# A running oneshot backup is "activating", which is-active does not count; settled does.
settled sparrow-backup.service || die "the nightly backup is running"
before=$("$CLI_LINK" "$SOCKET" status) || die "the running daemon does not answer status on $SOCKET"
STORE_ID=$(status_field store_id <<<"$before")
FORMAT=$(status_field store_format <<<"$before")
schema=$(status_field schema_version <<<"$before")
[[ $schema == "$FROM_SCHEMA" ]] || die "the running daemon reports schema $schema, not $FROM's $FROM_SCHEMA"
log "store_id before: $STORE_ID, format $FORMAT, schema $schema"
if ((NO_BACKUP)); then
    log "NO PRE-UPGRADE BACKUP (--no-backup): $NO_BACKUP_REASON"
    log "NO PRE-UPGRADE BACKUP: a rollback can only go forward on $TO; take the first online backup as soon as status answers"
else
    # The running daemon's backup state; an image without the online backup reports none.
    phase=$(python3 -I -c 'import json, sys; print(json.load(sys.stdin).get("backup", {}).get("phase", "absent"))' <<<"$before")
    case $phase in
        absent) die "the running daemon ($FROM) has no online backup, and no backup may stop it. \
Switch with --no-backup REASON, then take the first online backup once $TO answers" ;;
        unconfigured) die "the running daemon reports no backup_root; configure it and restart \
the daemon first, or switch with --no-backup REASON" ;;
    esac
    for dir in "$SPARROW_INTAKE" "$SPARROW_STATE/ship" "$SPARROW_STATE/ship-watches"; do
        [[ -d $dir ]] || die "the backup would keep $dir, which does not exist; set SPARROW_STATE or SPARROW_INTAKE"
    done
    log "pre-upgrade backup, online: the daemon keeps serving"
    PATH=$(dirname -- "$CLI_LINK"):$PATH "$LIBEXEC/sparrow-backup" "$SPARROW_CONFIG" \
        "$SPARROW_INTAKE" "$CONFIG_DIR" "$SPARROW_STATE/ship" "$SPARROW_STATE/ship-watches"
    fresh=''
    for dir in "$SPARROW_BACKUPS"/sets/*; do
        [[ -d $dir && ${dir##*/} =~ ^[0-9]+$ ]] || continue
        if [[ -z $fresh ]] || ((${dir##*/} > ${fresh##*/})); then fresh=$dir; fi
    done
    if [[ -z $fresh ]] || ! grep -qx "image=$FROM" "$fresh/SOURCE" \
        || ! grep -qx "store_id=$STORE_ID" "$fresh/SOURCE"; then
        die "the newest backup set ${fresh:-none} does not record image=$FROM and store_id=$STORE_ID"
    fi
    ln -s -- "sets/${fresh##*/}" "$BACKUP"
    log "pre-upgrade backup $BACKUP -> sets/${fresh##*/} ($(sed -n 's/^taken_utc=//p' "$fresh/SOURCE"))"
fi

# Which timers ran before this script stopped them: the abort path restores only those.
ship_timer_was_active=0 backup_timer_was_active=0
"${USER_SYSTEMCTL[@]}" is-active --quiet sparrow-ship.timer && ship_timer_was_active=1
"${USER_SYSTEMCTL[@]}" is-active --quiet sparrow-backup.timer && backup_timer_was_active=1
log "stop timers"
"${USER_SYSTEMCTL[@]}" stop sparrow-ship.timer sparrow-backup.timer
for _ in $(seq 360); do
    settled sparrow-ship.service && break
    sleep 5
done
if ! settled sparrow-ship.service || ! settled sparrow-backup.service; then
    # Restore only the timers that were active before they were stopped; sparrow-backup.timer is
    # never started here unless it was running, because it restarts only after a verified backup.
    restore=()
    ((ship_timer_was_active)) && restore+=(sparrow-ship.timer)
    ((backup_timer_was_active)) && restore+=(sparrow-backup.timer)
    ((${#restore[@]} == 0)) || "${USER_SYSTEMCTL[@]}" start "${restore[@]}"
    die "the shipper or the backup is still running; timers that were active before are started again (${restore[*]:-none}), nothing else changed"
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

# Wait for the first status answer, not for pressure "ready". pressure.phase is the disk-budget
# maintenance state (sparrow-core storage/pressure.rs): it opens ready, or awaitingReclamation
# when a recovered marker says so, and only data-budget admission and pressure passes move it.
# The schema 69 migrations never set it, so it is not a migration signal, and gating on it would
# wait on budget maintenance, which a switch cannot hurry. It is reported below instead.
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
T1=$EPOCHREALTIME
log "status answers; downtime $(awk -v a="$T1" -v b="$T0" 'BEGIN{printf "%.1f", a-b}') s"
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
# sparrow-backup.timer stays stopped: it restarts only after a first online backup verifies.
log "timers: ship $("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer), backup $("${USER_SYSTEMCTL[@]}" is-active sparrow-backup.timer) (left stopped on purpose)"
if ((NO_BACKUP)); then
    log "NO PRE-UPGRADE BACKUP was taken ($NO_BACKUP_REASON). Take the first online backup now:"
    printf '  %s %s backup   (then watch the backup state in status), or run sparrow-backup\n' "$CLI_LINK" "$SOCKET"
fi
log "sparrow-backup.timer is NOT started. Install the online sparrow-backup.service lines (deploy/README.md, \"Interim user unit\"), then:"
printf '  1. take the first online backup: %s %s backup (watch the backup state in status), or run sparrow-backup\n' "$CLI_LINK" "$SOCKET"
printf '  2. SPARROW_CONFIG=%s deploy/verify-backup.sh SET   (SET is the new set, %s/sets/ID)\n' "$SPARROW_CONFIG" "$SPARROW_BACKUPS"
printf '  3. only after step 2 exits 0: systemctl --user start sparrow-backup.timer\n'
log "switched to $TO. Any online backfill continues in the running daemon; watch it with:"
printf '  %s %s status   (migrations: done/total, failure)\n' "$CLI_LINK" "$SOCKET"
printf "  journalctl --user -u sparrow.service -g 'sparrowd: (migration|online migration failed)'\n"
