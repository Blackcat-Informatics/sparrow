#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Switch the operator's old user unit sparrow.service from one kept image pair to another, with
# the named pre-upgrade backup of the closed store taken in the stopped window. It serves until
# Sparrow issue #22 moves the store to the system unit, whose scripts (restart.sh,
# pre-upgrade-backup.sh) refuse the user unit. Ported from the operator's restart-bundle.sh; its
# predecessor restart-schema68.sh ran clean on the live host on 2026-11-07 (exit 0, 180 s down).
# Every check that can refuse runs before anything changes. Run it only on coordination's "go".
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/user-unit-upgrade.sh --from DAEMON_IMAGE --to DAEMON_IMAGE

Switch the user unit sparrow.service (systemctl --user) from the kept daemon image --from to
--to, both names under the libexec directory such as sparrowd-schema68-2aa56a9 and
sparrowd-schema69-3a64e08. Each CLI image is the daemon's name with sparrowd- replaced by
sparrow-; the schema numbers come from the names. Run as the operator, never as root.

Before anything changes it refuses unless: BACKUPS/pre-upgrade-FROM (and .partial) is absent;
the daemon link names --from; all four images are executable; the --to pair is byte-identical
to the kept pair in SPARROW_BIN; the --to daemon carries cap_net_bind_service (getcap: a human
with root runs setcap, this script never does); server.toml sets the ingest keys; the store has
copy-on-write off; the backups directory is on the store's filesystem, which has the store's
size plus 10 GiB free; no nightly backup is running; and the running daemon reports the --from
schema. Then it stops both timers and waits for the shipper, stops
the unit, takes the named pre-upgrade backup, switches the links, starts the unit, waits up to
600 s for the first status answer, checks the store identity and the --to schema, prints the
migrations, watches and ingest socket, and starts the timers again. It exits 0 without waiting
for the online backfill, which continues in the running daemon; watch it with
"sparrow SOCKET status" (migrations) and journalctl --user -u sparrow.service.

Environment (defaults are the user-unit layout):
  SPARROW_PREFIX   images in PREFIX/libexec/sparrow, CLI link PREFIX/bin/sparrow   ~/.local
  SPARROW_CONFIG   the daemon's server.toml, with ship.toml beside it; the store
                   (data_dir) and sockets are read from it       ~/.config/sparrow/server.toml
  SPARROW_BACKUPS  where the pre-upgrade backup is written       /SPARROW/backups
  SPARROW_BIN      the kept images the --to pair must match      <repository>/bin

Recovery depends on where it failed; the FAILED line names the unit and timer states.
- Before "stop timers": nothing changed. Fix the cause and rerun.
- From "stop timers" up to "switch links": the links still name FROM and the store is untouched.
  Remove BACKUPS/pre-upgrade-FROM.partial if it exists (a rerun refuses while it does), then
  start sparrow.service (if stopped) and both timers. A completed BACKUPS/pre-upgrade-FROM
  must be moved aside before a rerun.
- At "switch links", before "start sparrow.service": check both links with readlink; one may
  name TO and the other FROM. Point both at FROM and recover as in the step above, or both at
  TO and start sparrow.service, then continue as below.
- After "start sparrow.service": unless the FAILED line shows the unit inactive or failed, the
  daemon runs TO with both timers stopped, and TO may have accepted ingest and watch work.
  Go forward first: wait for "sparrow SOCKET status" to answer (a slow open passes 600 s
  without failing), confirm its store_id and the TO schema, then start both timers.
  Roll back only if TO cannot serve: stop sparrow.service; point the daemon link
  at FROM and the CLI link at the FROM CLI (an image restored from the backup's images/ needs
  setcap cap_net_bind_service=ep, run by a human with root); move the store aside and restore
  BACKUPS/pre-upgrade-FROM/store (cp -a --reflink=always into a chattr +C directory), which
  drops everything TO accepted; then start sparrow.service and both timers.
- Claims after a rollback or restore (docs/deployment.md, "Claims after a rollback"). Keep the
  set-aside TO store: it records every claim TO made. Before starting sparrow.service on the
  restored store, open the restored store with the FROM image and the set-aside store with the
  TO image, each query-only on its own scratch configuration and socket, then run
  deploy/reconcile-inflight.sh inflight --restored FROM_CLI SOCK1 --newer TO_CLI SOCK2.
  It lists each consume root's .sparrow-inflight/ and moves every entry whose claim the
  restored catalog does not know back to its root under its original batch name, by a hard
  link under that name and an unlink of the entry (same inode, bytes unchanged). Without this the daemon refuses to start with
  "unrecognized private claim entry" (or "private claim directory exceeds its bounded journal
  registry"). Stop the restored query-only daemon, start sparrow.service, then run
  deploy/reconcile-inflight.sh missing --restored FROM_CLI SOCKET --newer TO_CLI SOCK2 for
  claims TO published after the backup: the restored store still journals them, their watch
  failure reads "unpublished claimed source is missing", and every nightly backup waits for
  them. It reads each one's bytes back from the set-aside store, checks them against their
  catalog digest, re-ships them into the root as NAME.reshipped.EXT and retires the claim
  with sparrow SOCKET watch retire-claim WATCH_ID CLAIM_ID "REASON" (schema 70 and newer).
  A retired claim leaves pending_claims, status and watch list show it as
  retired-unrecoverable, and the daemon logs one line. Rerun it until no watch reports that
  failure. A FROM image older than schema 70 cannot retire (exit 3): the bytes are re-shipped
  and the claim stays pending until a schema 70 image retires it. A claim whose private
  source exists or whose publication is recorded is refused.
USAGE
}

FROM='' TO=''
while (($#)); do
    case $1 in
        -h | --help) usage; exit 0 ;;
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
[[ ! -e $BACKUP && ! -e $BACKUP.partial ]] || die "backup target $BACKUP (or .partial) exists"
current=$(readlink -- "$LIBEXEC/sparrowd" 2>/dev/null || true)
[[ $current == "$FROM" ]] || die "unexpected daemon link $LIBEXEC/sparrowd -> ${current:-none}, not $FROM"
refuse_schema_downgrade "$LIBEXEC/$TO"
for image in "$TO" "$TO_CLI" "$FROM" "$FROM_CLI"; do
    [[ -f $LIBEXEC/$image && -x $LIBEXEC/$image ]] || die "missing or not executable: $LIBEXEC/$image"
done
for image in "$TO" "$TO_CLI"; do
    cmp -s -- "$SPARROW_BIN/$image" "$LIBEXEC/$image" || die "$LIBEXEC/$image differs from $SPARROW_BIN/$image"
done
command -v getcap >/dev/null || die "getcap is missing; it checks the --to daemon's bind capability"
getcap -- "$LIBEXEC/$TO" 2>/dev/null | grep -q cap_net_bind_service \
    || die "$LIBEXEC/$TO lacks cap_net_bind_service; a human with root runs: setcap cap_net_bind_service=ep $LIBEXEC/$TO"
grep -q '^ingest_socket_path = ' "$SPARROW_CONFIG" || die "$SPARROW_CONFIG lacks the ingest keys"
STORE=$(config_value data_dir) || exit 1
SOCKET=$(config_value socket_path) || exit 1
INGEST_SOCKET=$(config_value ingest_socket_path) || exit 1
LEXICONS=$(config_value text_lexicon_dir) || exit 1
[[ -r $CONFIG_DIR/ship.toml ]] || die "cannot read $CONFIG_DIR/ship.toml; the backup keeps it"
no_copy_on_write "$STORE" || die "$STORE is not no-COW (lsattr shows no C)"
# The backup is a reflink copy, so it must share the store's filesystem; refuse now rather than
# after the stop.
backups_fs=$SPARROW_BACKUPS
[[ -d $backups_fs ]] || backups_fs=$(dirname -- "$SPARROW_BACKUPS")
# A filesystem's UUID spans its btrfs subvolumes, whose device numbers differ; a filesystem
# without a UUID falls back to the device number.
fs_identity() { findmnt -no UUID -T "$1" | grep . || stat -c 'dev:%d' -- "$1"; }
[[ $(fs_identity "$backups_fs") == "$(fs_identity "$STORE")" ]] \
    || die "$SPARROW_BACKUPS is not on $STORE's filesystem; the reflink backup needs one filesystem"
free=$(free_bytes "$STORE") || exit 1
need=$(du -s -B1 -- "$STORE" | cut -f1)
((free > need + 10 * 1024 ** 3)) || die "only $free bytes free beside $STORE; the backup needs $need bytes plus 10 GiB"

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

log "stop timers"
"${USER_SYSTEMCTL[@]}" stop sparrow-ship.timer sparrow-backup.timer
for _ in $(seq 360); do
    settled sparrow-ship.service && break
    sleep 5
done
if ! settled sparrow-ship.service || ! settled sparrow-backup.service; then
    "${USER_SYSTEMCTL[@]}" start sparrow-ship.timer sparrow-backup.timer
    die "the shipper or the backup is still running; timers started again, nothing else changed"
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

# The named pre-upgrade backup: once TO opens the store, the only way back to FROM's format. Its
# taken_utc is the compact stamp sparrow-backup writes, which lib.sh and verify-backup.sh read.
[[ -d $SPARROW_BACKUPS ]] || mkdir -m 0700 -- "$SPARROW_BACKUPS"
mkdir -m 0700 -- "$BACKUP.partial"
mkdir -m 0700 -- "$BACKUP.partial/store"
chattr +C -- "$BACKUP.partial/store"
cp -a --reflink=always -- "$STORE/." "$BACKUP.partial/store/"
diff <(cd -- "$STORE" && find . -printf '%P %y %s\n' | sort) \
    <(cd -- "$BACKUP.partial/store" && find . -printf '%P %y %s\n' | sort) \
    || die "the backup's file list differs from $STORE; $BACKUP.partial is kept for inspection"
mkdir -- "$BACKUP.partial/images" "$BACKUP.partial/config"
cp -a --reflink=auto -- "$LIBEXEC/$FROM" "$LIBEXEC/$FROM_CLI" "$BACKUP.partial/images/"
# Under the names verify-backup.sh reads back, whatever SPARROW_CONFIG is called.
cp -a -- "$SPARROW_CONFIG" "$BACKUP.partial/config/server.toml"
cp -a -- "$CONFIG_DIR/ship.toml" "$BACKUP.partial/config/ship.toml"
cat >"$BACKUP.partial/SOURCE" <<SOURCE
image=$FROM
cli=$FROM_CLI
store_id=$STORE_ID
schema_version=$FROM_SCHEMA
store_format=$FORMAT
text_lexicon_dir=$LEXICONS
source=$STORE (closed, user unit sparrow.service)
taken_utc=$(date -u +%Y%m%dT%H%M%SZ)
SOURCE
sync -f -- "$BACKUP.partial"
mv -T -- "$BACKUP.partial" "$BACKUP"
sync -f -- "$SPARROW_BACKUPS"
log "backup at $BACKUP ($(find "$BACKUP/store" -type f | wc -l) store files)"

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
"${USER_SYSTEMCTL[@]}" start sparrow-ship.timer sparrow-backup.timer
log "timers: ship $("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer), backup $("${USER_SYSTEMCTL[@]}" is-active sparrow-backup.timer)"
log "switched to $TO. Any online backfill continues in the running daemon; watch it with:"
printf '  %s %s status   (migrations: done/total, failure)\n' "$CLI_LINK" "$SOCKET"
printf "  journalctl --user -u sparrow.service -g 'sparrowd: (migration|online migration failed)'\n"
