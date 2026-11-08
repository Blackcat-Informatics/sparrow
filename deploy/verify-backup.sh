#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Verify a closed-store backup end to end (RECOVERY.md, "Verify a backup"). The backup's store is
# reflinked into a scratch directory below SPARROW_VERIFY (default /SPARROW/verify), and the image
# the backup recorded opens that copy in query-only mode, so no watch root is opened, no file is
# claimed and nothing is retired. The script checks the store identity, status and the stored
# payload chunk count, and, while the backup is younger than the retention window, a records window
# over its newest hour and one artifact's bytes against its catalog digest. It then stops the
# daemon with SIGINT and removes the copy. It prints one line per check and exits 1 at the first
# failed one. It never starts a daemon on the live store, socket or intake: the live data_dir and
# socket_path come from SPARROW_CONFIG, and it refuses without them, refuses a backup store that is
# or holds a symlink leading outside the backup, and refuses any scratch path that resolves under
# the live store, socket, intake or the backup.
#
# With --measure-migration it instead measures the online migrations (schema 69 and
# source-summary) on a writable copy (docs/deployment.md, "Measure the online migration"): the
# image opens the copy with `sparrowd --no-intake` (maintenance runs; no watch root, ingest
# socket, TCP listener or archive is opened) under a systemd user scope with the service unit's
# CPU, IO and memory limits, and the script logs machine-greppable `MEASURE` lines until status
# reports every migration complete. A disk guard refuses to start below 50 GB free and aborts
# below 40 GB, and the copy is removed however the run ends.
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/verify-backup.sh [--time-records] [--window FROM BEFORE] [--image DAEMON] BACKUP
       deploy/verify-backup.sh --measure-migration [--image DAEMON] BACKUP

Copy BACKUP/store into a scratch directory below SPARROW_VERIFY (default /SPARROW/verify), open
the copy query-only with the image BACKUP/SOURCE records, and check it. A backup counts as
verified only once this exits 0. SPARROW_CONFIG (default /etc/sparrow/server.toml) must be
readable: its data_dir and socket_path are the live paths the copy must never touch.

--time-records  after the checks, print five elapsed times of the records window
--window FROM BEFORE
                the RFC 3339 records window to check and time (default: the hour before
                taken_utc); it must be younger than the retention window
--image DAEMON  open the copy with DAEMON instead (its CLI is the sibling sparrow or
                sparrow-SUFFIX); a newer image may upgrade the disposable copy as it opens it
--measure-migration
                instead of verifying, open a writable copy with `sparrowd --no-intake` under
                `systemd-run --user --scope` (CPUQuota=100%, IOWeight=20, MemoryHigh=2G,
                MemoryMax=4G) and measure the online migrations until status reports them
                all complete. The filesystem holding SPARROW_VERIFY must have 50 GB free to
                start (SPARROW_MEASURE_MIN_FREE_GB may raise it); the run aborts below 40 GB
                (SPARROW_MEASURE_ABORT_FREE_GB may raise it). Output lines starting
                "verify-backup: MEASURE" carry the image and its sha256, the scope's applied
                limits, the start and end clock times, the seconds to the first status
                answer, done/total progress and the free space every minute, the total
                backfill seconds and live_records. The run fails on a migration failure
                (FAIL migrate), a durability failure (FAIL durability), or done/total
                unchanged for 600s after the first status (FAIL stall). The daemon's own
                migration lines are printed as "verify-backup: daemon:". The copy is
                removed however the run ends.
USAGE
}

TIME_RECORDS=0 MEASURE_MIGRATION=0 IMAGE_OVERRIDE='' WINDOW_FROM='' WINDOW_BEFORE=''
while (($#)); do
    case $1 in
        -h | --help) usage; exit 0 ;;
        --time-records) TIME_RECORDS=1; shift ;;
        --measure-migration) MEASURE_MIGRATION=1; shift ;;
        --image) IMAGE_OVERRIDE=${2:?--image needs a daemon image}; shift 2 ;;
        --window)
            WINDOW_FROM=${2:?--window needs FROM and BEFORE}
            WINDOW_BEFORE=${3:?--window needs FROM and BEFORE}
            shift 3
            ;;
        -*) printf 'verify-backup.sh: unknown option %s (see --help)\n' "$1" >&2; exit 2 ;;
        *) break ;;
    esac
done
(($# == 1)) || { usage >&2; exit 2; }
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

VERIFY_ROOT=${SPARROW_VERIFY:-/SPARROW/verify}
# The migration measurement's disk guard, in GB of 10^9 bytes. An override may only raise them.
GB=1000000000
MIN_FREE_GB=${SPARROW_MEASURE_MIN_FREE_GB:-50} ABORT_FREE_GB=${SPARROW_MEASURE_ABORT_FREE_GB:-40}
# Seconds between status polls while measuring; progress and free space are logged every minute.
POLL_SECONDS=15
# Seconds the migrations' done/total may stay unchanged after the first status before the run
# fails as stalled. A failing page already ends the run as FAIL migrate, so this catches stops
# that set no failure, such as the daemon pausing writes below its free-space floor. The worker
# retries a failed page every 60 seconds, and the live step-1 open averaged about 0.9 s per
# 64-graph page (34 minutes over 2,360 pages), so a healthy backfill moves every poll; ten retry
# intervals without movement is a stopped migration, and also the switch gate's whole budget.
STALL_SECONDS=600
# The scope limits as systemd reports them once applied (deploy/systemd/sparrow.service).
SCOPE_LIMITS='CPUQuotaPerSecUSec=1s IOWeight=20 MemoryHigh=2147483648 MemoryMax=4294967296'
if ((MEASURE_MIGRATION)); then
    ((TIME_RECORDS == 0)) && [[ -z $WINDOW_FROM ]] \
        || die "--measure-migration measures the migrations only; it takes no --time-records or --window"
    if [[ ! $MIN_FREE_GB =~ ^[0-9]+$ || ! $ABORT_FREE_GB =~ ^[0-9]+$ ]] \
        || ((MIN_FREE_GB < 50 || ABORT_FREE_GB < 40 || ABORT_FREE_GB >= MIN_FREE_GB)); then
        die "the disk guard needs SPARROW_MEASURE_MIN_FREE_GB >= 50 and SPARROW_MEASURE_ABORT_FREE_GB >= 40, below it"
    fi
    command -v systemd-run >/dev/null || die "--measure-migration needs systemd-run for the user scope that limits the daemon"
fi
BACKUP=$(realpath -e -- "$1") || die "backup $1 does not exist"
[[ -r $BACKUP/SOURCE ]] || die "$BACKUP holds no SOURCE; it is not a backup"
[[ ! -L $BACKUP/store ]] || die "$BACKUP/store is a symbolic link; a backup holds its store itself"
[[ -d $BACKUP/store ]] || die "$BACKUP holds no store; it is not a backup"
# sparrow-backup copies no links, and the copy keeps an absolute one, which the daemon would
# then follow out of the scratch directory; so a store holding any link is refused.
link=$(find "$BACKUP/store" -type l -print -quit) || die "cannot inspect the links under $BACKUP/store"
[[ -z $link ]] || die "$BACKUP/store holds the symbolic link $link; a backup's store holds no links"

# The paths the copy must never open: the live store and socket, the intake, and the backup.
[[ -r $SPARROW_CONFIG ]] || die "cannot read the live configuration $SPARROW_CONFIG (set \
SPARROW_CONFIG); its data_dir and socket_path are the paths a copy must never touch"
live_data=$(config_value data_dir) || exit 1
live_socket=$(config_value socket_path) || exit 1
# Also the store and intake the backup itself came from, in case SPARROW_INTAKE names another.
recorded=()
for key in data_dir intake; do
    value=$(sed -n "s/^$key=//p" "$BACKUP/SOURCE" | head -n 1)
    [[ -z $value ]] || recorded+=("$value")
done
if [[ -r $BACKUP/config/server.toml ]]; then
    value=$(sed -n -e '/^\[/q' -e 's/^data_dir *= *"\([^"]*\)".*/\1/p' "$BACKUP/config/server.toml" | head -n 1)
    [[ -z $value ]] || recorded+=("$value")
fi
guarded=()
for path in "$live_data" "$live_socket" "$SPARROW_INTAKE" "$BACKUP" "${recorded[@]}"; do
    resolved=$(realpath -m -- "$path") || exit 1
    guarded+=("$resolved")
done
# Refuse PATH (as RESOLVED) when it is, or lies under, a guarded path.
guard() {
    local path=$1 resolved=$2 protected
    for protected in "${guarded[@]}"; do
        [[ $resolved != "$protected" && $resolved != "$protected"/* ]] \
            || die "$path resolves to $resolved, under $protected; refusing to start a daemon there"
    done
}

pass() { printf 'verify-backup: PASS %s: %s\n' "$1" "$2"; }
fail() {
    printf 'verify-backup: FAIL %s: %s\n' "$1" "$2" >&2
    exit 1
}
# One SOURCE key; empty when the backup does not record it.
source_value() {
    sed -n "s/^$1=//p" "$BACKUP/SOURCE" | head -n 1
}
# One quoted or numeric top-level key of the backup's server configuration, read up to the first
# table header, so a key inside [[consume]] or [archive] never answers.
backup_config() {
    sed -n -e '/^\[/q' -e "s/^$1 *= *\"\\{0,1\\}\\([^\"#]*[^\" #]\\)\"\\{0,1\\}.*/\\1/p" \
        "$BACKUP/config/server.toml" | head -n 1
}
# Field of a JSON document on stdin, by key path.
json_field() {
    python3 -I -c 'import json,sys; v=json.load(sys.stdin)
for k in sys.argv[1:]: v=v[int(k)] if isinstance(v,list) else v[k]
print(json.dumps(v) if isinstance(v,(dict,list,bool)) or v is None else v)' "$@"
}

expected_store=$(source_value store_id)
[[ -n $expected_store ]] || die "$BACKUP/SOURCE records no store_id"
expected_chunks=$(source_value unique_payload_chunks)
taken=$(source_value taken_utc)
[[ -n $taken ]] || die "$BACKUP/SOURCE records no taken_utc"
# sparrow-backup writes a compact stamp (20261103T014038Z); older backups an RFC 3339 instant.
if [[ $taken =~ ^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})Z$ ]]; then
    taken="${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]}T${BASH_REMATCH[4]}:${BASH_REMATCH[5]}:${BASH_REMATCH[6]}Z"
fi
taken_seconds=$(date -u -d "$taken" +%s) || die "$BACKUP/SOURCE taken_utc $taken is not a time"
if [[ -n $WINDOW_FROM ]]; then
    from_seconds=$(date -u -d "$WINDOW_FROM" +%s) || die "--window FROM $WINDOW_FROM is not a time"
    before_seconds=$(date -u -d "$WINDOW_BEFORE" +%s) || die "--window BEFORE $WINDOW_BEFORE is not a time"
    ((from_seconds < before_seconds)) || die "--window FROM must be earlier than BEFORE"
else
    from_seconds=$((taken_seconds - 3600)) before_seconds=$((taken_seconds + 1))
fi
window_from=$(date -u -d "@$from_seconds" +%Y-%m-%dT%H:%M:%SZ) || exit 1
window_before=$(date -u -d "@$before_seconds" +%Y-%m-%dT%H:%M:%SZ) || exit 1

if [[ -n $IMAGE_OVERRIDE ]]; then
    daemon=$(realpath -e -- "$IMAGE_OVERRIDE") || die "image $IMAGE_OVERRIDE does not exist"
    name=${daemon##*/}
    case $name in
        sparrowd) cli=${daemon%/*}/sparrow ;;
        sparrowd-*) cli=${daemon%/*}/sparrow-${name#sparrowd-} ;;
        *) die "image $daemon is not named sparrowd or sparrowd-SUFFIX, so its CLI is unknown" ;;
    esac
else
    image=$(source_value image)
    [[ $image == sparrowd-* ]] || die "$BACKUP/SOURCE records no image; choose one with --image"
    cli_name=$(source_value cli)
    daemon=$BACKUP/images/$image
    cli=$BACKUP/images/${cli_name:-sparrow-${image#sparrowd-}}
fi
[[ -x $daemon && -x $cli ]] || die "image pair $daemon and $cli is missing or not executable"
# The usage line is the daemon's contract for query-only mode. An image without it would open
# the copy with its watch catalog and consume the live intake folders, so it is never started.
daemon_usage=$("$daemon" 2>&1 || true)
[[ $daemon_usage == *"--query-only"* ]] || die "$daemon predates query-only mode \
(it reports: $daemon_usage), so it cannot open a copy without claiming its watch roots. \
Verify with an image that has the mode (--image); a newer image may upgrade the disposable copy"
# The writable measurement needs the mode that runs maintenance but opens no watch root.
((MEASURE_MIGRATION == 0)) || [[ $daemon_usage == *"--no-intake"* ]] || die "$daemon has no \
--no-intake mode (it reports: $daemon_usage), so a writable copy would claim its watch roots; \
measure with an image that has the mode (--image)"

shopt -s nullglob
bundles=("$BACKUP"/lexicons/*/artifacts)
shopt -u nullglob
if ((${#bundles[@]} == 1)); then
    lexicons=${bundles[0]}
else
    ((${#bundles[@]} == 0)) || die "$BACKUP/lexicons holds ${#bundles[@]} bundles; expected one"
    lexicons=$(source_value text_lexicon_dir)
    [[ -n $lexicons ]] || lexicons=$(backup_config text_lexicon_dir)
fi
[[ -n $lexicons && -d $lexicons ]] || die "the lexicon bundle ${lexicons:-of $BACKUP} is missing"
lexicon_files=$(find "$lexicons" -maxdepth 1 -name '*.cbor' | wc -l) || exit 1
((lexicon_files == 5)) || die "$lexicons must hold the five native CBOR lexicons"

[[ -r $BACKUP/config/server.toml ]] || die "$BACKUP/config/server.toml is missing; the budgets come from it"
limits=()
for key in disk_budget maintenance_reserve max_records max_pin_members; do
    value=$(backup_config "$key")
    [[ $value =~ ^[0-9]+$ ]] || die "$BACKUP/config/server.toml sets no numeric $key"
    limits+=("$key = $value")
done
for key in min_free_bytes buffered_flush_interval_ms; do
    value=$(backup_config "$key")
    [[ -z $value ]] || limits+=("$key = $value")
done

verify_root=$(realpath -m -- "$VERIFY_ROOT") || exit 1
guard "$VERIFY_ROOT" "$verify_root"
# The free space the measurement guards is that of the filesystem holding SPARROW_VERIFY.
free_root=$verify_root
while [[ ! -e $free_root ]]; do
    free_root=$(dirname -- "$free_root")
done
# Log the free space for PHASE; with FLOOR (GB), also return 1 when it is below the floor.
# Return 2 when the free space cannot be read.
report_free() {
    local free floor=${2:-0}
    free=$(free_bytes "$free_root") || return 2
    printf 'verify-backup: MEASURE free_space phase=%s path=%s bytes=%s gb=%s\n' \
        "$1" "$free_root" "$free" $((free / GB))
    ((floor == 0 || free >= floor * GB))
}
if ((MEASURE_MIGRATION)); then
    report_free start "$MIN_FREE_GB" || {
        (($? == 1)) || die "cannot read the free space of $free_root; nothing was copied or started"
        die "$free_root has under the ${MIN_FREE_GB} GB free a writable copy needs to start; \
nothing was copied or started"
    }
fi
mkdir -p -- "$VERIFY_ROOT"
SCRATCH=$(mktemp -d -- "$VERIFY_ROOT/verify-XXXXXX") || exit 1
DATA=$SCRATCH/store SOCKET=$SCRATCH/store/verify.sock CONFIG=$SCRATCH/server.toml LOG=$SCRATCH/daemon.log
PID=''
cleanup() {
    local status=$?
    if [[ -n $PID ]] && kill -0 "$PID" 2>/dev/null; then
        kill -INT "$PID" 2>/dev/null || true
        wait "$PID" 2>/dev/null || true
    fi
    if ((status != 0)) && [[ -s $LOG ]]; then
        printf 'verify-backup: daemon log:\n' >&2
        tail -n 20 -- "$LOG" >&2 || true
    fi
    # The daemon's own migration lines (page counts, progress, failed pages) are evidence too, and
    # the log goes with the copy, so keep them in the run's output however it ended.
    if ((MEASURE_MIGRATION)) && [[ -s $LOG ]]; then
        sed -n 's/^sparrowd: \(migration \|online migration failed\)/verify-backup: daemon: &/p' -- "$LOG" || true
    fi
    rm -rf -- "$SCRATCH"
    ((MEASURE_MIGRATION == 0)) || report_free end || true
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Every path the daemon will own resolves outside the guarded paths, before and after the copy.
scratch=$(realpath -e -- "$SCRATCH") || exit 1
guard "$SCRATCH" "$scratch"
require_socket_length "$SOCKET"

# The copy's directory takes the backup store's copy-on-write attribute, since btrfs refuses a
# reflink between a copy-on-write file and a no-copy-on-write one.
mkdir -- "$DATA"
match_copy_on_write "$BACKUP/store" "$DATA"
cp -a --reflink=always -- "$BACKUP/store/." "$DATA/" \
    || die "cannot reflink $BACKUP/store below $VERIFY_ROOT; it must be on the backup's btrfs filesystem, and every file in it must share the store directory's copy-on-write attribute (lsattr)"
{
    ((MEASURE_MIGRATION)) || printf 'query_only = true\n'
    printf 'socket_path = "%s"\ndata_dir = "%s"\ntext_lexicon_dir = "%s"\n' "$SOCKET" "$DATA" "$lexicons"
    printf 'max_concurrent_requests = 4\nmax_rpc_connections = 16\n'
    printf '%s\n' "${limits[@]}"
} >"$CONFIG"
for path in "$DATA" "$CONFIG"; do
    resolved=$(realpath -e -- "$path") || die "the scratch copy $path is missing"
    guard "$path" "$resolved"
done
socket_dir=$(realpath -e -- "${SOCKET%/*}") || exit 1
guard "$SOCKET" "$socket_dir/${SOCKET##*/}"
[[ ! -L $DATA && ! -e $SOCKET ]] || die "the scratch copy $DATA is a link or already holds $SOCKET"

# Seconds elapsed since START (date +%s%N), to the millisecond.
elapsed() {
    local now
    now=$(date +%s%N) || return 1
    printf '%d.%03d' $(((now - $1) / 1000000000)) $(((now - $1) / 1000000 % 1000))
}
# The number of stored watch roots a status document on stdin reports open.
open_watches() {
    json_field consume_watches | python3 -I -c 'import json,sys; print(sum(w["directories"] != "0" and w["directories"] != 0 for w in json.load(sys.stdin)))'
}

if ((MEASURE_MIGRATION)); then
    image_sha=$(sha256sum -- "$daemon") || exit 1
    printf 'verify-backup: MEASURE image=%s sha256=%s\n' "$daemon" "${image_sha%% *}"
    started=$(date +%s%N) || exit 1
    printf 'verify-backup: MEASURE start_utc=%s\n' "$(date -u -d "@$((started / 1000000000))" +%Y-%m-%dT%H:%M:%SZ)"
    # A scope runs the daemon as this process (systemd-run execs it), so PID is the daemon's.
    systemd-run --user --scope --quiet --collect --unit="sparrow-${SCRATCH##*/}" \
        -p CPUQuota=100% -p IOWeight=20 -p MemoryHigh=2G -p MemoryMax=4G \
        -- "$daemon" --config "$CONFIG" --no-intake 2>"$LOG" >/dev/null &
    PID=$!
    # No readiness ceiling: how long the first open takes is what this mode measures.
    first='' next_minute=$((SECONDS + 60)) failures=0 progress='status=unanswered' moved=0
    while :; do
        kill -0 "$PID" 2>/dev/null || fail open "the daemon exited (see its log below)"
        if ((SECONDS >= next_minute)); then
            next_minute=$((next_minute + 60))
            printf 'verify-backup: MEASURE progress elapsed_seconds=%s %s\n' "$(elapsed "$started")" "$progress"
            report_free minute "$ABORT_FREE_GB" || {
                (($? == 1)) || fail disk "cannot read the free space of $free_root; stopping the \
daemon and removing $SCRATCH"
                fail disk "$free_root fell under the ${ABORT_FREE_GB} GB abort floor; stopping the \
daemon and removing $SCRATCH"
            }
        fi
        if ! grep -q '^sparrowd: store=' -- "$LOG"; then
            sleep 1
            continue
        fi
        if ! status=$("$cli" "$SOCKET" status 2>"$SCRATCH/status.err"); then
            # Before the first answer each attempt warms the copy's cache; after it, three
            # failures running end the run.
            if [[ -n $first ]]; then
                failures=$((failures + 1))
                ((failures < 3)) || fail status "status did not answer three times running: $(cat -- "$SCRATCH/status.err")"
            fi
            printf 'verify-backup: status did not answer (%s); retrying\n' "$(cat -- "$SCRATCH/status.err")"
            sleep 1
            continue
        fi
        failures=0
        if [[ -z $first ]]; then
            first=$(elapsed "$started") || exit 1
            printf 'verify-backup: MEASURE first_status_seconds=%s\n' "$first"
            store=$(json_field store_id <<<"$status") || exit 1
            [[ $store == "$expected_store" ]] || fail store-id "the copy is $store, but SOURCE records $expected_store"
            mode=$(json_field query_only <<<"$status") || exit 1
            opened=$(open_watches <<<"$status") || exit 1
            if [[ $mode != false ]] || ! grep -q 'query_only=false intake=false' -- "$LOG" || ((opened)); then
                fail no-intake "the daemon does not report no-intake mode, or $opened stored watch roots are open"
            fi
            pass no-intake "$store is writable with no intake; no stored watch root is open"
            scope="sparrow-${SCRATCH##*/}.scope"
            applied=$(systemctl --user show "$scope" -p CPUQuotaPerSecUSec -p IOWeight -p MemoryHigh \
                -p MemoryMax | sort | paste -sd ' ') || fail scope "systemctl cannot show $scope"
            printf 'verify-backup: MEASURE scope=%s %s\n' "$scope" "$applied"
            [[ $applied == "$SCOPE_LIMITS" ]] || fail scope "$scope runs with $applied, not $SCOPE_LIMITS"
        fi
        durability=$(json_field durability failure <<<"$status") || exit 1
        [[ $durability == null ]] || fail durability "the copy reports durability failure $durability"
        mapfile -t state < <(python3 -I -c 'import json,sys
rows=json.load(sys.stdin)["migrations"]
failed=[r for r in rows if r["failure"] is not None]
print("failed" if failed else "complete" if all(r["watermark"] is None for r in rows) else "pending")
print(" ".join("%s=%s/%s" % (r["name"], r["done"], r["total"]) for r in rows))
print("; ".join("%s: %s" % (r["name"], r["failure"]) for r in failed))' <<<"$status")
        ((${#state[@]} == 3)) || fail status "status reports no readable migrations"
        if [[ ${state[1]} != "$progress" ]]; then
            moved=$SECONDS
        elif ((SECONDS - moved >= STALL_SECONDS)); then
            fail stall "the migrations have not moved from ${state[1]} in ${STALL_SECONDS}s"
        fi
        progress=${state[1]}
        [[ ${state[0]} != failed ]] || fail migrate "${state[2]}"
        [[ ${state[0]} != complete ]] || break
        sleep "$POLL_SECONDS"
    done
    printf 'verify-backup: MEASURE backfill_seconds=%s\n' "$(elapsed "$started")"
    printf 'verify-backup: MEASURE end_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'verify-backup: MEASURE live_records=%s\n' "$(json_field live_records <<<"$status")"
    pass migrate "every migration is complete with no failure: $progress"
    kill -INT "$PID"
    stopped=0
    wait "$PID" || stopped=$?
    PID=''
    ((stopped == 0)) || fail shutdown "the daemon exited with status $stopped after SIGINT"
    # Retention runs in a writable copy, so its closing count need not equal SOURCE's.
    grep -Eq "^sparrowd: closed store=$store unique_payload_chunks=[0-9]+\$" -- "$LOG" \
        || fail shutdown "the daemon did not log its closing count"
    pass shutdown "the daemon stopped cleanly; removing $SCRATCH"
    printf 'verify-backup: migration of %s measured\n' "$BACKUP"
    exit 0
fi

"$daemon" --config "$CONFIG" --query-only 2>"$LOG" >/dev/null &
PID=$!
# Readiness is the startup line, printed once the store is open and the socket bound. Status
# counts every live record, which on a copy whose files are not yet cached can outlast the
# 60-second connection lease, so it is not the readiness probe; each attempt warms the cache.
deadline=$((SECONDS + READY_SECONDS))
until grep -q '^sparrowd: store=' -- "$LOG"; do
    kill -0 "$PID" 2>/dev/null || fail open "the daemon exited before it answered on $SOCKET"
    ((SECONDS < deadline)) || fail open "the daemon did not answer within ${READY_SECONDS}s"
    sleep 0.2
done
for attempt in 1 2 3; do
    if status=$("$cli" "$SOCKET" status 2>"$SCRATCH/status.err"); then
        break
    fi
    ((attempt < 3)) || fail status "status did not answer in three attempts: $(cat -- "$SCRATCH/status.err")"
    printf 'verify-backup: status attempt %d did not answer (%s); retrying\n' "$attempt" "$(cat -- "$SCRATCH/status.err")"
done
mode=$(json_field query_only <<<"$status") || exit 1
if [[ $mode != true ]] || ! grep -q 'query_only=true' -- "$LOG"; then
    fail query-only "the daemon does not report query-only mode; its watch roots could be opened"
fi
opened=$(open_watches <<<"$status") || exit 1
((opened == 0)) || fail query-only "$opened stored watch roots are open"
pass query-only "status and startup report query_only; no stored watch root is open"

store=$(json_field store_id <<<"$status") || exit 1
[[ $store == "$expected_store" ]] || fail store-id "the copy is $store, but SOURCE records $expected_store"
pass store-id "$store"

phase=$(json_field pressure phase <<<"$status") || exit 1
durability=$(json_field durability failure <<<"$status") || exit 1
[[ $phase == ready && $durability == null ]] \
    || fail status "pressure phase $phase, durability failure $durability"
pass status "ready, schema $(json_field schema_version <<<"$status"), format $(json_field store_format <<<"$status")"

# Unique payload chunks change only when evidence is retired, which a query-only daemon never
# does, so the copy holds exactly the count sparrow-backup recorded, however old it is.
chunks=$(json_field unique_payload_chunks <<<"$status") || exit 1
unrecorded=0
if [[ -z $expected_chunks ]]; then
    unrecorded=1
    printf 'verify-backup: SKIP payload-chunks: %s chunks; SOURCE records no unique_payload_chunks, so this backup or its image predates the count\n' "$chunks"
elif [[ $chunks == "$expected_chunks" ]]; then
    pass payload-chunks "$chunks unique payload chunks, as SOURCE recorded"
else
    fail payload-chunks "the copy holds $chunks unique payload chunks, but SOURCE recorded $expected_chunks"
fi

# Records expire by the reader's clock, so the window answers only while all of it is younger
# than the retention window the daemon reports.
retention=$(json_field retention_seconds <<<"$status") || exit 1
[[ $retention =~ ^[0-9]+$ ]] || fail status "status reports no retention_seconds"
now_seconds=$(date -u +%s) || exit 1
window=(records --received-from "$window_from" --received-before "$window_before")
if ((now_seconds < from_seconds + retention)); then
    page=$("$cli" "$SOCKET" "${window[@]}") || fail records "the records window did not answer"
    rows=$(json_field rows <<<"$page" | python3 -I -c 'import json,sys; print(len(json.load(sys.stdin)))') || exit 1
    ((rows > 0)) || fail records "no records were received between $window_from and $window_before"
    pass records "$rows rows on the first page between $window_from and $window_before"

    artifact=$(json_field rows 0 artifact_id <<<"$page") || exit 1
    # The CLI has no artifact command; MCP artifact_get reads the same typed catalog row.
    metadata=$(printf '%s\n' \
        '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"verify-backup","version":"1"}}}' \
        '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"artifact_get\",\"arguments\":{\"store_id\":\"$store\",\"id\":\"$artifact\"}}}" \
        | "$cli" "$SOCKET" mcp --stdio) || fail artifact "MCP artifact_get did not answer"
    read -r payload length < <(python3 -I -c 'import json,sys
for line in sys.stdin:
    reply=json.loads(line)
    if reply.get("id")==2:
        value=reply["result"]["structuredContent"]
        print(value["payload_id"], value["byte_length"])' <<<"$metadata") || fail artifact "no catalog row for $artifact"
    [[ $payload == blake3:* && $length =~ ^[0-9]+$ ]] || fail artifact "malformed catalog row for $artifact"
    bytes=$SCRATCH/artifact.bytes chunk=$SCRATCH/artifact.chunk
    : >"$bytes"
    for ((offset = 0; offset < length; offset += 1048576)); do
        size=$((length - offset < 1048576 ? length - offset : 1048576))
        "$cli" "$SOCKET" bytes "$store" "$artifact" "$offset" "$size" "$chunk" >/dev/null \
            || fail artifact "reading $artifact at offset $offset failed"
        cat -- "$chunk" >>"$bytes"
    done
    digest=$(b3sum --no-names -- "$bytes") || exit 1
    [[ blake3:$digest == "$payload" ]] || fail artifact "$artifact reads back as blake3:$digest, but its catalog records $payload"
    pass artifact "$artifact: $length bytes read back to $payload"
else
    ((TIME_RECORDS == 0)) || fail time-records "the window from $window_from has aged out of the ${retention}s retention window"
    printf 'verify-backup: SKIP records: the window from %s is older than the %ss retention window, so its records have expired\n' "$window_from" "$retention"
    printf 'verify-backup: SKIP artifact: no retained window to take an artifact from\n'
fi

if ((TIME_RECORDS)); then
    printf 'verify-backup: timing: sparrow SOCKET %s\n' "${window[*]}"
    for run in 1 2 3 4 5; do
        started=$(date +%s%N) || exit 1
        "$cli" "$SOCKET" "${window[@]}" >/dev/null || fail time-records "run $run failed"
        took=$(elapsed "$started") || exit 1
        printf 'verify-backup: time-records run %d: %s s\n' "$run" "$took"
    done
fi

kill -INT "$PID"
stopped=0
wait "$PID" || stopped=$?
PID=''
((stopped == 0)) || fail shutdown "the daemon exited with status $stopped after SIGINT"
# The closing count sparrow-backup records comes from this same line of the live daemon.
grep -qx "sparrowd: closed store=$store unique_payload_chunks=$chunks" -- "$LOG" \
    || fail shutdown "the daemon did not log its closing count of $chunks unique payload chunks"
pass shutdown "the daemon stopped cleanly and logged $chunks unique payload chunks; removing $SCRATCH"
if ((unrecorded)); then
    printf 'verify-backup: NOT VERIFIED: SOURCE records no unique_payload_chunks; take a new backup with sparrow-backup\n' >&2
    exit 1
fi
printf 'verify-backup: %s verified\n' "$BACKUP"
