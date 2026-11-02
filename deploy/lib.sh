# shellcheck shell=bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Shared by start.sh, stop.sh, restart.sh, install.sh and check.sh; sourced, never run. Every
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

# A string value of a top-level key in the daemon's TOML configuration.
config_value() {
    local key=$1 value
    [[ -r $SPARROW_CONFIG ]] || die "cannot read $SPARROW_CONFIG"
    value=$(sed -n "s/^$key *= *\"\\([^\"]*\\)\".*/\\1/p" "$SPARROW_CONFIG" | head -n 1)
    [[ -n $value ]] || die "$SPARROW_CONFIG sets no $key"
    printf '%s\n' "$value"
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

# Refuse an image whose schema is older than the current one's: the current image may have upgraded
# the store's format, which an older image cannot open (RECOVERY.md, "Rollback").
refuse_schema_downgrade() {
    local daemon=$1 current new old
    current=$(readlink -- "$LIBEXEC/sparrowd" 2>/dev/null || true)
    [[ -n $current ]] || return 0
    new=${daemon##*/sparrowd-schema}; new=${new%%-*}
    old=${current#sparrowd-schema}; old=${old%%-*}
    [[ $new =~ ^[0-9]+$ && $old =~ ^[0-9]+$ ]] \
        || die "cannot read the schema from image names ${daemon##*/} and $current"
    ((new >= old)) || die "${daemon##*/} is schema $new, older than the current $current; \
it cannot open a store that image upgraded. Fix forward, or restore the backup taken before the \
upgrade (RECOVERY.md, \"Rollback\")"
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

unit_active() {
    "${SYSTEMCTL[@]}" is-active --quiet "$SPARROW_UNIT"
}

# The daemon's typed status over its private socket, as JSON on stdout.
status_json() {
    "$LIBEXEC/sparrow" "$(config_value socket_path)" status
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
receipt() {
    local status
    status=$(status_json)
    printf 'receipt: unit=%s state=%s pid=%s\n' "$SPARROW_UNIT" \
        "$("${SYSTEMCTL[@]}" is-active "$SPARROW_UNIT" || true)" \
        "$("${SYSTEMCTL[@]}" show -p MainPID --value "$SPARROW_UNIT")"
    printf 'receipt: daemon=%s cli=%s previous=%s\n' "$(readlink -- "$LIBEXEC/sparrowd")" \
        "$(readlink -- "$LIBEXEC/sparrow")" "$(readlink -- "$LIBEXEC/sparrowd.previous" 2>/dev/null || echo none)"
    printf 'receipt: store_id=%s schema=%s pressure=%s durability_failure=%s watches=%s\n' \
        "$(status_field store_id <<<"$status")" "$(status_field schema_version <<<"$status")" \
        "$(status_field pressure phase <<<"$status")" "$(status_field durability failure <<<"$status")" \
        "$(python3 -I -c 'import json,sys; print(len(json.load(sys.stdin)["consume_watches"]))' <<<"$status")"
}
