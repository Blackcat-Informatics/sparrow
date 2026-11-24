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

DEPLOY_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd -- "$DEPLOY_DIR/.." && pwd)
SPARROW_UNIT=${SPARROW_UNIT:-sparrow.service}
SPARROW_SCOPE=${SPARROW_SCOPE:-system}
SPARROW_PREFIX=${SPARROW_PREFIX:-/usr/local}
SPARROW_CONFIG=${SPARROW_CONFIG:-/etc/sparrow/server.toml}
SPARROW_BIN=${SPARROW_BIN:-$REPO_DIR/bin}
LIBEXEC=$SPARROW_PREFIX/libexec/sparrow
CLI_LINK=$SPARROW_PREFIX/bin/sparrow
# The daemon takes up to TimeoutStopSec (120 s) to drain; startup can wait longer for disk space.
READY_SECONDS=${SPARROW_READY_SECONDS:-120}

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

# Run a host-changing command, or print it when staging (DEST set, as install.sh does for
# SPARROW_DESTDIR).
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

# Respect a start limit set by the installed unit or a local override.
# Diagnose repeated failures before the operator resets it (RECOVERY.md, "Unit start-limit hit").
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
cannot open a store that image upgraded. Fix forward (RECOVERY.md, \"Rollback\")"
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
    if [[ -n ${DEST:-} ]]; then
        swap_link "$CLI_LINK" ../libexec/sparrow/sparrow
    else
        swap_link "$CLI_LINK" "$LIBEXEC/sparrow"
    fi
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

# Field of a JSON document on stdin, by key path (a list index is a number); an object, a
# list, a boolean or null prints as JSON.
json_field() {
    python3 -I -c 'import json,sys; v=json.load(sys.stdin)
for k in sys.argv[1:]: v=v[int(k)] if isinstance(v,list) else v[k]
print(json.dumps(v) if isinstance(v,(dict,list,bool)) or v is None else v)' "$@"
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

# Copy-on-write is off for the store. On btrfs, a file or directory created in a directory
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
copy_on_write_files() (
    set -o pipefail
    cd -- "$1" || exit 1
    # lsattr separates results with line breaks. Print only the flags for a path
    # that contains a line break, including one in a parent directory.
    find . -type f \( -path $'*\n*' -exec bash -c '
        for file; do
            flags=$(lsattr -d -- "$file") || exit 1
            printf "%s\n" "${flags%% *}"
        done
    ' bash {} + -o -exec lsattr -d -- {} + \) | awk '$1 !~ /C/ { count++ } END { print count + 0 }'
)

# Switch copy-on-write off for DIR where its filesystem has the attribute. It is never a failure
# for the caller. A directory that already holds copy-on-write files is left as it is, because new
# files would inherit the attribute and the store would be mixed, so no reflink copy of it would
# work. Such a directory is converted with convert_store_no_cow.
switch_off_copy_on_write() {
    local dir=$1 output count
    if [[ -n ${DEST:-} ]]; then
        note "staged, not run: chattr +C $dir"
        return 0
    fi
    on_btrfs "$dir" || return 0
    no_copy_on_write "$dir" && return 0
    if ! count=$(copy_on_write_files "$dir"); then
        note "could not check existing file attributes in $dir, so its attributes stay unchanged"
        return 0
    fi
    if ((count > 0)); then
        note "$dir holds copy-on-write files; convert it while closed (deploy/copy-on-write.sh convert $dir)"
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

# Refuse while any process holds a file under the store DIR open. find reads each descriptor's
# link target itself, where a shell loop forks once per descriptor on the host.
refuse_open_store() {
    local holder
    local pattern=${1//[\\*?[]/\\&}   # -lname reads glob characters; the path is literal
    holder=$(find /proc/[0-9]*/fd -maxdepth 1 -lname "$pattern/*" -print -quit 2>/dev/null || true)
    [[ -z $holder ]] || die "process ${holder#/proc/} still holds a file in $1 open; stop the daemon first"
}

# The listing the store copy is compared by: every directory (name, type, mode) and every other
# entry (name, type, size, mode, link target) under DIR, sockets left out, sorted. A directory's
# size counts its entry names, so it is not listed.
store_listing() (
    set -o pipefail
    cd -- "$1" || exit 1
    find . ! -type s \( -type d -printf '%P\0%y %m\0\0' -o -printf '%P\0%y %s %m\0%l\0' \) |
        while IFS= read -r -d '' path && IFS= read -r -d '' metadata && IFS= read -r -d '' target; do
            printf '%q %s %q\0' "$path" "$metadata" "$target"
        done | LC_ALL=C sort -z
)

store_listings_match() (
    local lists
    lists=$(mktemp -d) || exit 1
    trap 'rm -f -- "$lists/source" "$lists/copy"; rmdir -- "$lists"' EXIT
    store_listing "$1" > "$lists/source" || exit 1
    store_listing "$2" > "$lists/copy" || exit 1
    cmp -s -- "$lists/source" "$lists/copy"
)

store_file_bytes_match() (
    cd -- "$1" || exit 1
    find . -type f -exec bash -c '
        copy=$1
        shift
        for file; do
            cmp -s -- "$file" "$copy/$file" || {
                printf "the copy of %q differs from the source\n" "$file" >&2
                exit 1
            }
        done
    ' bash "$2" {} +
)

# Whether a reflink copy of the store DIR can share its extents.
can_reflink_store() {
    local count
    on_btrfs "$1" && no_copy_on_write "$1" || return 1
    count=$(copy_on_write_files "$1") || return 1
    ((count == 0))
}

# The checks that precede a store copy, all before anything changes: DEST is a new name in an
# existing directory outside SOURCE, and the filesystem has the room. Prints "reflink" or "full",
# the method keep_store_copy will use, and the bytes SOURCE holds. Free space must exceed the
# store's size plus 10 GiB in both methods: a reflink copy writes almost nothing at first, but it
# holds every extent the running store later rewrites or compacts away, so in the end it can cost
# the whole store.
check_store_copy() {
    local source dest parent method need free
    source=$(realpath -e -- "$1") || die "$1 does not exist"
    dest=$2
    [[ $dest == /* && $dest != *..* ]] || die "the store copy $dest is not an absolute path without .."
    [[ ! -e $dest && ! -L $dest ]] || die "the store copy $dest already exists; choose a new name"
    [[ ! -e $dest.partial ]] || die "$dest.partial is left from an earlier attempt; inspect it and remove it"
    parent=$(realpath -e -- "$(dirname -- "$dest")") || die "the directory of $dest does not exist"
    [[ $parent/ != "$source"/* ]] || die "the store copy $dest lies inside $source"
    [[ -d $parent && -w $parent && -x $parent ]] || die "$parent is not a directory this user can create in"
    if can_reflink_store "$source"; then method=reflink; else method=full; fi
    if [[ $method == reflink ]]; then
        [[ $(findmnt -no SOURCE -T "$source" | sed 's/\[.*//') == "$(findmnt -no SOURCE -T "$parent" | sed 's/\[.*//')" ]] \
            || die "$parent is not on the filesystem of $source; a reflink copy needs the same one"
    fi
    need=$(du -s -B1 -- "$source" | cut -f1)
    free=$(free_bytes "$parent") || exit 1
    ((free > need + 10 * 1024 ** 3)) \
        || die "only $free bytes free in $parent; a copy of $source (${need} bytes) needs that plus 10 GiB"
    printf '%s %s\n' "$method" "$need"
}

# Copy the closed store SOURCE to the new directory DEST: a reflink copy when check_store_copy says
# reflink, a full copy with a byte comparison of every file when it says full. The copy is made
# in DEST.partial, listed against SOURCE and renamed to DEST only when the listing matches, so a
# DEST that exists is a complete copy. Nothing in SOURCE changes. The Unix sockets a closed store
# can leave are neither copied nor compared.
keep_store_copy() {
    local source dest method files bytes
    source=$(realpath -e -- "$1") || die "$1 does not exist"
    dest=$2
    method=$(check_store_copy "$source" "$dest") || exit 1
    method=${method%% *}
    refuse_open_store "$source"
    mkdir -- "$dest.partial"
    match_copy_on_write "$source" "$dest.partial"
    if [[ $method == reflink ]]; then
        cp -a --reflink=always -- "$source/." "$dest.partial/" \
            || die "reflink copy of $source failed; $source is unchanged and $dest.partial is kept for inspection"
    else
        cp -a --reflink=never -- "$source/." "$dest.partial/" \
            || die "copy of $source failed; $source is unchanged and $dest.partial is kept for inspection"
    fi
    find "$dest.partial" -type s -delete
    store_listings_match "$source" "$dest.partial" \
        || die "the copy's file list differs from $source or cannot be read; $source is unchanged and $dest.partial is kept"
    if [[ $method == full ]]; then
        store_file_bytes_match "$source" "$dest.partial" \
            || die "file comparison failed; $source is unchanged and $dest.partial is kept"
    fi
    files=$(find "$dest.partial" -type f -printf . | wc -c)
    bytes=$(find "$dest.partial" -type f -printf '%s\n' | awk '{s += $1} END {print s + 0}')
    sync -f -- "$dest.partial"
    mv -T -- "$dest.partial" "$dest"
    sync -f -- "$dest"
    note "kept a $method copy of $source at $dest: $files files, $bytes bytes"
}

# Check conversion prerequisites that do not require a closed store. Print the
# mode for the converter. Restart checks it before stopping and conversion repeats it.
check_store_conversion() {
    local data=$1 partial parent filesystem flags count report need free status
    [[ -d $data ]] || die "$data is not a directory"
    filesystem=$(findmnt -no FSTYPE -T "$data") || die "cannot identify the filesystem of $data"
    if [[ $filesystem != btrfs ]]; then
        printf '%s\n' other-filesystem
        return 0
    fi

    partial=$data.no-cow.partial
    [[ ! -e $partial && ! -L $partial ]] \
        || die "$partial is left from an earlier conversion; preserve both directories until you identify the original (deploy/RECOVERY.md)"
    flags=$(lsattr -d -- "$data") || die "cannot read copy-on-write attributes of $data"
    if [[ ${flags%% *} == *C* ]]; then
        count=$(copy_on_write_files "$data") || die "cannot check file attributes under $data"
        if ((count == 0)); then
            printf '%s\n' already-converted
            return 0
        fi
    fi

    parent=$(dirname -- "$data")
    [[ -r $parent && -w $parent && -x $parent ]] || die "cannot create and synchronize a sibling of $data in $parent"
    if mountpoint -q -- "$data"; then
        die "$data is a mount point and cannot be exchanged with its sibling"
    else
        status=$?
        ((status == 32)) || die "cannot determine whether $data is a mount point"
    fi
    mv --exchange --no-copy -T --help >/dev/null 2>&1 \
        || die "put GNU mv with --exchange and --no-copy first in PATH before converting $data"
    report=$(du -s -B1 -- "$data") || die "cannot measure the size of $data"
    need=${report%%[[:space:]]*}
    [[ $need =~ ^[0-9]+$ ]] || die "du reports no size for $data"
    free=$(free_bytes "$parent") || exit 1
    ((free > need + 10 * 1024 ** 3)) \
        || die "only $free bytes free beside $data; converting it needs $need bytes plus 10 GiB"
    printf '%s\n' convert
}

# Copy the closed store, verify it, and exchange the two sibling directories.
# Between the exchange and retention, the partial name holds the original.
convert_store_no_cow() {
    local data partial kept mode count files parent
    data=$(realpath -e -- "$1") || die "$1 does not exist"
    mode=$(check_store_conversion "$data") || exit 1
    case $mode in
        other-filesystem) note "$data is not on btrfs; it has no copy-on-write to switch off"; return 0 ;;
        already-converted) note "copy-on-write is already off for $data and every file in it"; return 0 ;;
    esac
    refuse_open_store "$data"
    parent=$(dirname -- "$data")
    partial=$data.no-cow.partial
    kept=$data.cow-$(date -u +%Y%m%dT%H%M%SZ)
    [[ ! -e $kept && ! -L $kept ]] || die "$kept already exists; retain it and retry with a new timestamp"

    mkdir -- "$partial" || die "cannot create $partial; $data is unchanged"
    chattr +C -- "$partial" || die "cannot switch copy-on-write off for $partial; $data is unchanged"
    # cp -a copies the mode, owner and ACLs. Files inherit the sibling's attribute.
    cp -a --reflink=never -- "$data/." "$partial/" \
        || die "copying $data failed; $data is unchanged and $partial is kept for inspection"
    find "$partial" -type s -delete || die "cannot remove stale sockets from $partial; $data is unchanged"
    store_listings_match "$data" "$partial" \
        || die "the copy's file list differs from $data or cannot be read; $data is unchanged and $partial is kept"
    store_file_bytes_match "$data" "$partial" \
        || die "file comparison failed; $data is unchanged and $partial is kept"
    count=$(copy_on_write_files "$partial") || die "cannot check file attributes in $partial; $data is unchanged"
    if ((count > 0)) || ! no_copy_on_write "$partial"; then
        die "$partial or files in it still have copy-on-write; $data is unchanged"
    fi
    files=$(find "$partial" -type f -printf . | wc -c) || die "cannot count files in $partial; $data is unchanged"
    sync -f -- "$partial" || die "cannot synchronize $partial; $data is unchanged"
    mv --exchange --no-copy -T -- "$data" "$partial" \
        || die "exchange of $data and $partial did not report success; preserve both and inspect them (deploy/RECOVERY.md)"
    # Candidate bytes are already durable. Persist the sibling names without
    # flushing unrelated writers on the same filesystem again.
    sync -- "$parent" \
        || die "cannot synchronize the exchange; preserve $data and the original at $partial"
    mv --no-copy --no-clobber -T -- "$partial" "$kept" && [[ ! -e $partial && ! -L $partial ]] \
        || die "converted store at $data; retaining the original did not report success; inspect $partial and $kept"
    sync -f -- "$data" \
        || die "cannot synchronize retention of the original; preserve $data and $kept"
    note "converted $data: $files files with copy-on-write off; the original is kept at $kept"
}
