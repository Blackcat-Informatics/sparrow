# shellcheck shell=bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Shared by the deploy scripts; sourced, never run. Every
# location below has a production default and an environment override, so the same scripts drive
# a test instance (deploy/test-instance.sh) without editing them.
#
#   SPARROW_UNIT      the daemon's unit                     sparrow.service
#   SPARROW_SCOPE     system or user (systemctl --user)     system
#   SPARROW_PREFIX    images, CLI link and lexicons         /usr/local
#   SPARROW_CONFIG    the daemon's configuration            /etc/sparrow/server.toml
#   SPARROW_BIN       the kept images (make install-bin)    <repository>/bin
#   SPARROW_BACKUPS   the online backup root, on the NAS    /net/synology/archives/sparrow
#   SPARROW_INTAKE    the watched intake tree                /SPARROW/intake
#   SPARROW_PRE_UPGRADE_MAX_HOURS  the oldest pre-upgrade backup accepted        24

DEPLOY_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd -- "$DEPLOY_DIR/.." && pwd)
SPARROW_UNIT=${SPARROW_UNIT:-sparrow.service}
SPARROW_SCOPE=${SPARROW_SCOPE:-system}
SPARROW_PREFIX=${SPARROW_PREFIX:-/usr/local}
SPARROW_CONFIG=${SPARROW_CONFIG:-/etc/sparrow/server.toml}
SPARROW_BIN=${SPARROW_BIN:-$REPO_DIR/bin}
SPARROW_BACKUPS=${SPARROW_BACKUPS:-/net/synology/archives/sparrow}
SPARROW_INTAKE=${SPARROW_INTAKE:-/SPARROW/intake}
LIBEXEC=$SPARROW_PREFIX/libexec/sparrow
CLI_LINK=$SPARROW_PREFIX/bin/sparrow
# The daemon takes up to TimeoutStopSec (120 s) to drain; startup can wait longer for disk space.
READY_SECONDS=${SPARROW_READY_SECONDS:-120}
PRE_UPGRADE_MAX_HOURS=${SPARROW_PRE_UPGRADE_MAX_HOURS:-24}

case $SPARROW_SCOPE in
    system) SYSTEMCTL=(systemctl) ;;
    user) SYSTEMCTL=(systemctl --user) ;;
    *) printf 'deploy: SPARROW_SCOPE must be system or user, not %s\n' "$SPARROW_SCOPE" >&2; exit 2 ;;
esac

die() {
    printf '%s: %s\n' "${0##*/}" "$*" >&2
    exit 1
}

note() {
    printf '%s: %s\n' "${0##*/}" "$*"
}

# While the old user unit exists, user scope with the default unit name would address it. systemctl
# reads "sparrow" as "sparrow.service", so both spellings are refused.
[[ $SPARROW_SCOPE != user || ${SPARROW_UNIT%.service} != sparrow ]] \
    || die "SPARROW_SCOPE=user needs SPARROW_UNIT naming a test unit (deploy/test-instance.sh env), never sparrow.service"

# Host changes made so far, for the final report and for report_partial: CHANGED lists files and
# links written, RAN the host commands run (their effects are not itemized).
CHANGED=() RAN=()

# Run a host-changing command, or print it when staging (DEST set, as install.sh and uninstall.sh
# do for SPARROW_DESTDIR).
host() {
    if [[ -n ${DEST:-} ]]; then
        note "staged, not run: $*"
    else
        "$@"
        RAN+=("$*")
    fi
}

# An EXIT trap: a failure part way still reports what had changed.
report_partial() {
    local status=$?
    if ((status != 0 && ${#CHANGED[@]} + ${#RAN[@]})); then
        printf '%s: failed after changing:\n' "${0##*/}" >&2
        printf '  %s\n' "${CHANGED[@]}" "${RAN[@]/#/ran: }" >&2
    fi
}

# The final report of a script that changes the host.
report_changes() {
    if ((${#CHANGED[@]})); then
        printf '%s: changed:\n' "${0##*/}"
        printf '  %s\n' "${CHANGED[@]}"
    else
        printf '%s: nothing changed\n' "${0##*/}"
    fi
    if ((${#RAN[@]})); then
        printf '%s: ran (effects not itemized; README.md, "Layout", gives the result):\n' "${0##*/}"
        printf '  %s\n' "${RAN[@]}"
    fi
}

# The operators: the accounts config/sysusers-sparrow.conf adds to the sparrow group.
operators() {
    sed -n 's/^m \([^ ]*\) sparrow$/\1/p' "$DEPLOY_DIR/config/sysusers-sparrow.conf"
}

# What keeps OPERATOR's old user unit sparrow.service in the way, one line each: enabled, and
# running. Read-only. Run as the operator or as root (through the operator's user manager).
old_user_unit_problems() {
    local operator=$1 home wants state uid manager
    home=$(getent passwd "$operator" | cut -d: -f6)
    [[ -n $home ]] || die "no account $operator"
    wants=$home/.config/systemd/user/default.target.wants/sparrow.service
    [[ ! -e $wants ]] || printf 'enabled (%s)\n' "$wants"
    if [[ $(id -un) == "$operator" ]]; then
        state=$(systemctl --user is-active sparrow.service || true)
    else
        state=$(systemctl --user --machine="$operator@" is-active sparrow.service 2>/dev/null || true)
        if [[ -z $state ]]; then
            uid=$(id -u "$operator")
            # No answer is fine only when the operator has no user manager to run the unit. The
            # state, not is-active's exit status: a manager still starting is "activating".
            manager=$(systemctl show -p ActiveState --value "user@$uid.service")
            [[ $manager == inactive || $manager == failed ]] \
                || die "cannot query $operator's user manager ($manager) for sparrow.service"
        fi
    fi
    case $state in
        active | activating | deactivating | reloading | refreshing) printf '%s\n' "$state" ;;
    esac
}

# A string value of a top-level key in the daemon's TOML configuration.
config_value() {
    local key=$1 value
    [[ -r $SPARROW_CONFIG ]] || die "cannot read $SPARROW_CONFIG"
    value=$(sed -n "s/^$key *= *\"\\([^\"]*\\)\".*/\\1/p" "$SPARROW_CONFIG" | head -n 1)
    [[ -n $value ]] || die "$SPARROW_CONFIG sets no $key"
    printf '%s\n' "$value"
}

# A Unix socket path must fit sockaddr_un's sun_path: 107 bytes and the terminating NUL. The daemon
# refuses a longer one only after it has created the store, so check before starting it.
require_socket_length() {
    local socket=$1
    ((${#socket} <= 107)) || die "socket path $socket is ${#socket} bytes; a Unix socket path is \
limited to 107. Choose a shorter data_dir (for a test instance, a shorter SCRATCH)"
}

# The kept image pair for a revision, or the newest pair in ./bin. Prints "DAEMON CLI".
select_images() {
    local rev=${1:-} daemon cli
    if [[ -n $rev ]]; then
        daemon=$(find "$SPARROW_BIN" -maxdepth 1 -type f -name "sparrowd-schema*-$rev" | head -n 1)
    else
        daemon=$(find "$SPARROW_BIN" -maxdepth 1 -type f -name 'sparrowd-schema*-*' -printf '%T@ %p\n' \
            | sort -n | tail -n 1 | cut -d' ' -f2-)
    fi
    [[ -n $daemon ]] || die "no sparrowd image${rev:+ for $rev} in $SPARROW_BIN; run make install-bin"
    cli=$SPARROW_BIN/sparrow-${daemon##*/sparrowd-}
    [[ -f $cli ]] || die "no CLI image $cli to match $daemon"
    printf '%s %s\n' "$daemon" "$cli"
}

# Copy a kept image into LIBEXEC under its own name: a temporary copy, compared, then renamed, so
# a half-copied image never carries the final name. An installed image is never overwritten.
install_image() {
    local source=$1 target candidate
    target=$LIBEXEC/${source##*/}
    if [[ -e $target ]]; then
        cmp -s -- "$source" "$target" || die "$target exists and differs from $source; refusing to overwrite an image"
        printf '%s\n' "$target"
        return
    fi
    install -d -m 0755 -- "$LIBEXEC"
    candidate=$(mktemp "$LIBEXEC/.candidate.XXXXXX")
    cp --reflink=auto -- "$source" "$candidate"
    chmod 0755 -- "$candidate"
    cmp -s -- "$source" "$candidate" || { rm -f -- "$candidate"; die "copy of $source does not match"; }
    sync -- "$candidate"
    mv -T -n -- "$candidate" "$target"
    [[ ! -e $candidate ]] || { rm -f -- "$candidate"; die "$target appeared while installing it"; }
    printf '%s\n' "$target"
}

# The image executes on this host: each binary prints its usage line when given no arguments.
verify_image() {
    local image=$1 expected=$2 output
    output=$("$image" 2>&1 || true)
    [[ $output == *"$expected"* ]] || die "$image did not run on this host: $output"
}

# Replace a symbolic link atomically (rename over it).
swap_link() {
    local link=$1 target=$2 next
    next=$(dirname -- "$link")/.${link##*/}.next
    ln -sfn -- "$target" "$next"
    mv -T -- "$next" "$link"
}

# Refuse to start a unit that spent its start limit: the repeated failure needs diagnosis first,
# and resetting it is the operator's decision (RECOVERY.md, "Unit start-limit hit").
refuse_start_limit() {
    [[ $("${SYSTEMCTL[@]}" show -p Result --value "$SPARROW_UNIT") != start-limit-hit ]] \
        || die "$SPARROW_UNIT hit its start limit; diagnose it (RECOVERY.md, \"Unit start-limit hit\"), \
then run: ${SYSTEMCTL[*]} reset-failed $SPARROW_UNIT"
}

# The schema number N in an image name sparrowd-schemaN-REV.
image_schema() {
    local name=${1##*/}
    name=${name#sparrowd-schema}
    name=${name%%-*}
    [[ $name =~ ^[0-9]+$ ]] || die "cannot read the schema from image name ${1##*/}"
    printf '%s\n' "$name"
}

# Refuse an image whose schema is older than the current one's: the current image may have upgraded
# the store's format, which an older image cannot open (RECOVERY.md, "Rollback"). The rule compares
# the schema numbers in the image names; it does not read the store's format.
refuse_schema_downgrade() {
    local daemon=$1 current new old
    current=$(readlink -- "$LIBEXEC/sparrowd" 2>/dev/null || true)
    [[ -n $current ]] || return 0
    new=$(image_schema "$daemon") || exit 1
    old=$(image_schema "$current") || exit 1
    ((new >= old)) || die "${daemon##*/} is schema $new, older than the current $current; it \
cannot open a store that image upgraded. Fix forward, or restore the pre-upgrade backup \
(RECOVERY.md, \"Rollback\")"
}

# Raising the schema may upgrade the store's format, after which only a backup taken before can go
# back. Require the named pre-upgrade backup of the current image (README.md, "Upgrades and
# rollback"); the backup rotation never prunes it. It must also be fresh: taken after the image link
# was last set (start.sh and restart.sh set it at every start, a restore included), so a copy that
# predates a restore is refused, and within PRE_UPGRADE_MAX_HOURS of now.
require_pre_upgrade_backup() {
    local daemon=$1 current backup new old taken taken_at linked_at
    current=$(readlink -- "$LIBEXEC/sparrowd" 2>/dev/null || true)
    [[ -n $current ]] || return 0
    new=$(image_schema "$daemon") || exit 1
    old=$(image_schema "$current") || exit 1
    ((new > old)) || return 0
    backup=$SPARROW_BACKUPS/pre-upgrade-$current
    grep -qx "image=$current" "$backup/SOURCE" 2>/dev/null || die "${daemon##*/} raises the schema \
from $old to $new; take the named pre-upgrade backup first: sudo deploy/pre-upgrade-backup.sh \
(README.md, \"Upgrades and rollback\")"
    # The RFC 3339 instant the daemon started the online backup (sparrow-backup's taken_utc).
    taken=$(sed -n 's/^taken_utc=//p' "$backup/SOURCE")
    taken_at=$(date -u -d "$taken" +%s 2>/dev/null) && [[ -n $taken ]] \
        || die "$backup/SOURCE records no taken_utc; retire it and take a new one with deploy/pre-upgrade-backup.sh"
    # The link's own time (no -L): when start.sh or restart.sh last pointed it at the image.
    linked_at=$(stat -c %Y -- "$LIBEXEC/sparrowd")
    ((taken_at >= linked_at)) || die "$backup was taken at $taken, before $current was last started \
($(date -u -d "@$linked_at" +%Y%m%dT%H%M%SZ)); it may predate a restore. Retire it \
(mv -T $backup $backup.retired-$taken) and take a new one: sudo deploy/pre-upgrade-backup.sh"
    ((EPOCHSECONDS - taken_at <= PRE_UPGRADE_MAX_HOURS * 3600)) || die "$backup was taken at $taken, \
more than $PRE_UPGRADE_MAX_HOURS hours ago; retire it (mv -T $backup $backup.retired-$taken) and take \
a new one: sudo deploy/pre-upgrade-backup.sh"
    note "pre-upgrade backup $backup (taken $taken) holds the store as $current left it"
}

# Point the stable names at an image pair, keeping the pair they named as .previous.
select_current() {
    local daemon=$1 cli=$2 old
    old=$(readlink -- "$LIBEXEC/sparrowd" 2>/dev/null || true)
    if [[ -n $old && $old != "${daemon##*/}" ]]; then
        swap_link "$LIBEXEC/sparrowd.previous" "$old"
        swap_link "$LIBEXEC/sparrow.previous" "$(readlink -- "$LIBEXEC/sparrow")"
    fi
    swap_link "$LIBEXEC/sparrowd" "${daemon##*/}"
    swap_link "$LIBEXEC/sparrow" "${cli##*/}"
    install -d -m 0755 -- "$(dirname -- "$CLI_LINK")"
    swap_link "$CLI_LINK" "$LIBEXEC/sparrow"
}

# Hold the backup's lock for the rest of the script, so a backup and an image switch cannot
# interleave: the backup records the image the link names. Refuses rather than waits, because an
# online backup can run for as long as its copy to the NAS takes.
hold_backup_lock() {
    # No backup directory yet means no backup has ever run, so none can be running.
    [[ -d $SPARROW_BACKUPS ]] || return 0
    exec {BACKUP_LOCK}>>"$SPARROW_BACKUPS/.lock"
    flock -n "$BACKUP_LOCK" || die "a backup holds $SPARROW_BACKUPS/.lock; wait for it to finish \
(journalctl -u sparrow-backup.service) and run this again"
}

# The unit's ActiveState. is-active's exit status is 0 only for "active", so gating on it treats a
# unit that is starting, stopping or between automatic restarts as stopped.
unit_state() {
    "${SYSTEMCTL[@]}" show -p ActiveState --value "$SPARROW_UNIT"
}

# Refuse unless the unit is settled, active or stopped: switching image links under a unit that is
# starting, draining or between automatic restarts would race its start.
require_settled() {
    local state
    state=$(unit_state)
    case $state in
        active | inactive | failed) ;;
        *) die "$SPARROW_UNIT is $state; wait until it is active or stopped (deploy/stop.sh stops it)" ;;
    esac
}

unit_active() {
    "${SYSTEMCTL[@]}" is-active --quiet "$SPARROW_UNIT"
}

# The daemon's typed status over its private socket, as JSON on stdout.
status_json() {
    local socket
    socket=$(config_value socket_path) || exit 1
    "$LIBEXEC/sparrow" "$socket" status
}

# One field of status_json's output.
status_field() {
    python3 -I -c 'import json,sys; v=json.load(sys.stdin); [v := v[k] for k in sys.argv[1:]]; print(v)' "$@"
}

# Wait until the started daemon answers status on its socket, or the unit fails.
wait_ready() {
    local deadline=$((SECONDS + READY_SECONDS))
    while ((SECONDS < deadline)); do
        if ! "${SYSTEMCTL[@]}" is-active --quiet "$SPARROW_UNIT"; then
            "${SYSTEMCTL[@]}" status --no-pager --lines=20 "$SPARROW_UNIT" >&2 || true
            die "$SPARROW_UNIT is not active"
        fi
        if status_json >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    die "$SPARROW_UNIT did not answer status within ${READY_SECONDS}s (a full disk makes it wait: see RECOVERY.md)"
}

# Stop the daemon through its SIGINT drain and report what it still held.
drain_stop() {
    local socket main
    socket=$(config_value socket_path)
    local state
    state=$("${SYSTEMCTL[@]}" is-active "$SPARROW_UNIT" || true)
    case $state in
        active) ;;
        inactive | failed)
            note "$SPARROW_UNIT is $state"
            return 0 ;;
        *)
            # Activating covers a restart loop: stopping cancels the pending restart.
            "${SYSTEMCTL[@]}" stop "$SPARROW_UNIT"
            note "$SPARROW_UNIT was $state (no drain: the daemon was not serving); stopped"
            return 0 ;;
    esac
    if status_json >"$WORK/before-stop.json" 2>/dev/null; then
        python3 -I -c 'import json, sys
for w in json.load(open(sys.argv[1]))["consume_watches"]:
    if w["pending_claims"]:
        print("watch", w["id"], "holds", w["pending_claims"], "pending claim(s); they resume at the next start")' "$WORK/before-stop.json"
    else
        note "status unavailable before stop; stopping anyway"
    fi
    main=$("${SYSTEMCTL[@]}" show -p MainPID --value "$SPARROW_UNIT")
    "${SYSTEMCTL[@]}" stop "$SPARROW_UNIT"
    if [[ $("${SYSTEMCTL[@]}" show -p Result --value "$SPARROW_UNIT") != success ]]; then
        "${SYSTEMCTL[@]}" status --no-pager --lines=20 "$SPARROW_UNIT" >&2 || true
        die "$SPARROW_UNIT did not stop cleanly (pid $main); read RECOVERY.md before starting it"
    fi
    if [[ -e $socket ]]; then
        die "the daemon exited but left $socket; it is removed at the next start under the store lock, never by hand"
    fi
    note "$SPARROW_UNIT stopped through its drain (pid $main); socket removed"
}

# The receipt every lifecycle script ends with.
# Each value is captured on its own line, so a failure stops the script rather than printing blank.
receipt() {
    local status state pid daemon cli previous fields store schema pressure watches durability
    status=$(status_json)
    state=$("${SYSTEMCTL[@]}" is-active "$SPARROW_UNIT" || true)
    pid=$("${SYSTEMCTL[@]}" show -p MainPID --value "$SPARROW_UNIT")
    daemon=$(readlink -- "$LIBEXEC/sparrowd")
    cli=$(readlink -- "$LIBEXEC/sparrow")
    previous=$(readlink -- "$LIBEXEC/sparrowd.previous" 2>/dev/null || echo none)
    fields=$(python3 -I -c 'import json, sys
v = json.load(sys.stdin)
print(v["store_id"], v["schema_version"], v["pressure"]["phase"], len(v["consume_watches"]),
      v["durability"]["failure"])' <<<"$status")
    # The failure, last, may hold spaces.
    read -r store schema pressure watches durability <<<"$fields"
    printf 'receipt: unit=%s state=%s pid=%s\n' "$SPARROW_UNIT" "$state" "$pid"
    printf 'receipt: daemon=%s cli=%s previous=%s\n' "$daemon" "$cli" "$previous"
    printf 'receipt: store_id=%s schema=%s pressure=%s durability_failure=%s watches=%s\n' \
        "$store" "$schema" "$pressure" "$durability" "$watches"
}

# Copy-on-write is off for the store (Patrick, 2026-10-15: "COW off everywhere"), by Katamari's
# mechanism (katamari-io copy_on_write.rs). On btrfs, a file or directory created in a directory
# that carries the no-copy-on-write attribute (chattr +C) inherits it. So the attribute is set on
# a directory before anything is written there. A file that already holds data keeps
# copy-on-write, so an existing store is rewritten by copy while it is closed
# (convert_store_no_cow). btrfs refuses a reflink between a copy-on-write file and a
# no-copy-on-write one, and cp --reflink=auto then copies every byte. So a copy's directory takes
# its source's attribute (match_copy_on_write).

on_btrfs() {
    [[ $(findmnt -no FSTYPE -T "$1" 2>/dev/null) == btrfs ]]
}

no_copy_on_write() {
    local flags
    flags=$(lsattr -d -- "$1" 2>/dev/null) || return 1
    [[ ${flags%% *} == *C* ]]
}

# The number of regular files under $1 that still have copy-on-write.
copy_on_write_files() {
    find "$1" -type f -exec lsattr -- {} + 2>/dev/null | awk '$1 !~ /C/' | wc -l
}

# Switch copy-on-write off for DIR where its filesystem has the attribute. It is never a failure
# for the caller. A directory that already holds copy-on-write files is left as it is, because new
# files would inherit the attribute and the store would be mixed, so no reflink copy of it would
# work. Such a directory is converted with convert_store_no_cow.
switch_off_copy_on_write() {
    local dir=$1 output
    on_btrfs "$dir" || return 0
    no_copy_on_write "$dir" && return 0
    if (($(copy_on_write_files "$dir"))); then
        note "$dir holds copy-on-write files; convert it while closed (deploy/copy-on-write.sh convert $dir)"
        return 0
    fi
    if [[ -n ${DEST:-} ]]; then
        note "staged, not run: chattr +C $dir"
        return 0
    fi
    output=$(chattr +C -- "$dir" 2>&1) || note "copy-on-write stays on for $dir: $output"
}

# Give the empty directory DIR the copy-on-write attribute of SOURCE's directory, so that a reflink
# copy of SOURCE into DIR shares extents instead of copying every byte.
match_copy_on_write() {
    local source=$1 dir=$2 flag=-C output
    on_btrfs "$dir" || return 0
    no_copy_on_write "$source" && flag=+C
    output=$(chattr "$flag" -- "$dir" 2>&1) || die "cannot chattr $flag $dir to match $source: $output"
}

# Bytes available to an unprivileged writer on the filesystem holding PATH. Run it in a command
# substitution followed by "|| exit 1", since die there ends only the subshell.
free_bytes() {
    local free
    free=$(df -B1 --output=avail -- "$1" | tail -n 1) || die "cannot read the free space of $1"
    free=${free//[[:space:]]/}
    [[ $free =~ ^[0-9]+$ ]] || die "df reports no free space for $1"
    printf '%s\n' "$free"
}

# Rewrite the closed store DATA so that it and every file in it have copy-on-write off. The copy
# is made in a no-copy-on-write sibling, its file list, sizes and bytes are compared with DATA, and
# it then takes DATA's name. The original is kept, renamed DATA.cow-STAMP, for the operator to
# remove once a backup of the converted store has verified.
convert_store_no_cow() {
    local data partial kept need free link files
    data=$(realpath -e -- "$1") || die "$1 does not exist"
    [[ -d $data ]] || die "$data is not a directory"
    if ! on_btrfs "$data"; then
        note "$data is not on btrfs; it has no copy-on-write to switch off"
        return 0
    fi
    if no_copy_on_write "$data" && (($(copy_on_write_files "$data") == 0)); then
        note "copy-on-write is already off for $data and every file in it"
        return 0
    fi
    for link in /proc/[0-9]*/fd/*; do
        [[ $(readlink -- "$link" 2>/dev/null) != "$data"/* ]] \
            || die "process ${link#/proc/} still holds a file in $data open; stop the daemon first"
    done
    partial=$data.no-cow.partial
    kept=$data.cow-$(date -u +%Y%m%dT%H%M%SZ)
    [[ ! -e $partial ]] || die "$partial is left from an earlier attempt; inspect and remove it"
    need=$(du -s -B1 -- "$data" | cut -f1)
    free=$(free_bytes "${data%/*}") || exit 1
    ((free > need + 10 * 1024 ** 3)) \
        || die "only $free bytes free beside $data; converting it needs $need bytes plus 10 GiB"
    mkdir -- "$partial"
    chattr +C -- "$partial" || die "cannot switch copy-on-write off for $partial; $data is unchanged"
    # cp -a copies DATA's mode, owner and ACLs onto PARTIAL; the files inherit PARTIAL's attribute.
    # A socket is not data: one is left only by an unclean exit, and the daemon creates its socket
    # at start. So sockets are neither kept nor compared.
    cp -a --reflink=never -- "$data/." "$partial/" \
        || die "copying $data failed; $data is unchanged and $partial is kept for inspection"
    find "$partial" -type s -delete
    # A directory's size counts its entry names, so it is not compared.
    diff -q <(cd -- "$data" && find . ! -type s \( -type d -printf '%P %y %m\n' -o -printf '%P %y %s %m %l\n' \) | sort) \
        <(cd -- "$partial" && find . \( -type d -printf '%P %y %m\n' -o -printf '%P %y %s %m %l\n' \) | sort) >/dev/null \
        || die "the copy's file list differs from $data; $data is unchanged and $partial is kept"
    while IFS= read -r -d '' file; do
        cmp -s -- "$data/$file" "$partial/$file" \
            || die "the copy of $file differs from $data; $data is unchanged and $partial is kept"
    done < <(cd -- "$data" && find . -type f -printf '%P\0')
    if (($(copy_on_write_files "$partial"))) || ! no_copy_on_write "$partial"; then
        die "$partial or files in it still have copy-on-write; $data is unchanged"
    fi
    files=$(find "$partial" -type f | wc -l)
    sync -f -- "$partial"
    mv -T -- "$data" "$kept"
    mv -T -- "$partial" "$data"
    sync -f -- "$data"
    note "converted $data: $files files with copy-on-write off; the original is kept at $kept"
}
