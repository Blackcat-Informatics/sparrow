#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Verify an online backup end to end (RECOVERY.md, "Verify a backup"). BACKUP is one backup set,
# BACKUP_ROOT/sets/ID on the NAS (/net/synology/archives/sparrow), written by sparrow-backup beside
# the engine backup ID in BACKUP_ROOT/engine. Every file its MANIFEST.b3 lists (the set's files and
# the engine files the backup references) is checked with b3sum against the NAS, and the set's file
# list compared both ways. The image the backup recorded then restores engine backup ID into a
# scratch working copy below SPARROW_VERIFY (default /SPARROW/verify) with `sparrowd
# --restore-backup`, which checks every file's checksum as it reads it; the copy is not a backup and
# is removed however the run ends. That image opens the copy in query-only mode, so no watch root
# is opened, no file is claimed and nothing is retired. The script checks the store identity and
# status, and, while the backup is younger than the retention window, a records window over its
# newest hour and one artifact's bytes against its catalog digest. It then stops the daemon with
# SIGINT and removes the copy. It prints one line per check and exits 1 at the first failed one.
# It never starts a daemon on the live store, socket or intake: the live data_dir and socket_path
# come from SPARROW_CONFIG, and it refuses without them, and refuses any scratch path that resolves
# under the live store, socket, intake or the backup root.
#
# With --measure-migration it instead measures the online migrations (schema 69,
# source-summary and, when the open found the source timeline cache stale, source-context) on a
# writable copy (docs/deployment.md, "Measure the online migration"): the
# image opens the copy with `sparrowd --no-intake` (maintenance runs; no watch root, ingest
# socket, TCP listener or archive is opened) under a systemd user scope with the service unit's
# CPU, IO and memory limits, and the script logs machine-greppable `MEASURE` lines until status
# reports every migration complete. A disk guard refuses to start below 50 GB free and aborts
# below 40 GB, and the copy is removed however the run ends.
#
# With --with-traffic as well, the copy runs as the live store does, with its watches and an
# ingest socket, opened with `sparrowd --private-intake` so that every watch consumes a private
# root below the scratch directory and never the live roots its catalog names. What the copy
# received in the hour before the backup replays from its own catalog at its recorded rate:
# each watch's intake into that watch's private root, through the repository's shipper as
# live received it, and the direct producer artifacts over the copy's own ingest socket. Status,
# records and sparql run every 30 seconds until the backfill completes
# (deploy/libexec/sparrow-measure-load). The run then checks the switch gate's thresholds and
# exits 4 when one fails. --rehearse-rollback then rehearses the rollback's
# claim reconciliation (deploy/reconcile-inflight.sh) on scratch copies of the measured copy.
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/verify-backup.sh [--time-records] [--window FROM BEFORE] [--image DAEMON] BACKUP
       deploy/verify-backup.sh --measure-migration [--image DAEMON] BACKUP
       deploy/verify-backup.sh --measure-migration --with-traffic [--rehearse-rollback] --image DAEMON BACKUP

BACKUP is a backup set, BACKUP_ROOT/sets/ID. Check every file BACKUP/MANIFEST.b3 lists against
the NAS, restore engine backup ID into a scratch directory below SPARROW_VERIFY (default
/SPARROW/verify) with the image BACKUP/SOURCE records, open the copy query-only and check it. The
scratch filesystem must keep SPARROW_VERIFY_MARGIN_GB (default 40, may only be raised) GB free after the
copy of the backup's engine_bytes, and the run prints the figures; the copy is removed however
the run ends. A backup counts as verified only once this exits 0. Exit 1 is a failed check; exit
2 is a usage error. SPARROW_CONFIG (default /etc/sparrow/server.toml) must be readable: its
data_dir and socket_path are the live paths the copy must never touch.

--time-records  after the checks, print five elapsed times of the records window
--window FROM BEFORE
                the RFC 3339 records window to check and time (default: the hour before
                taken_utc); it must be younger than the retention window
--image DAEMON  open the copy with DAEMON instead (its CLI is the sibling sparrow or
                sparrow-SUFFIX); a newer image may upgrade the disposable copy as it opens it
--measure-migration
                instead of verifying, open a writable copy with `sparrowd --no-intake` under
                `systemd-run --user --scope` (CPUQuota=100%, IOWeight=20, MemoryHigh=2G,
                MemoryMax=4G, and the Nice of the user unit SPARROW_MEASURE_UNIT, default
                sparrow.service) and measure the online migrations until status reports them
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
--with-traffic  with --measure-migration: open the copy with `sparrowd --private-intake DIR`
                instead, with its watches on private roots below the scratch directory and
                its own ingest socket, and run recorded intake and query traffic for the
                whole backfill (docs/deployment.md, "Measure the online migration with
                intake and traffic"). It needs BACKUP/config/sparrow/ship.toml (the set's
                copy of /etc/sparrow) and an image whose usage line has --private-intake.
                The window is the hour before the backup's taken_utc
                (SPARROW_MEASURE_REPLAY_SECONDS may widen it up to 86400). Every artifact
                the copy received then is read from a query-only prep copy, a second
                restore of the set, and replayed at its recorded time, looping: a watch's
                artifact into that watch's private root, through deploy/libexec/sparrow-ship
                every 300 s when ship.toml has a source named after the root, and any other
                artifact over the copy's ingest socket. A ship.toml source with no watch
                root or with nothing in the window fails the run (FAIL prep) before the
                measured open. Status, records and sparql run every 30 s. Extra MEASURE
                lines give the backup's own record count (backup_live_records, from the
                backup's image query-only, or the measured image query-only on the prep
                copy when that is exact, else pending), the records the replay added
                (replay_added), per-source replay counts, each watch's released, due and
                completed items, pages per second overall and per migration, each call
                kind's p50, p95, max, calls, failures and calls over 60 s, publication
                deadline expiries, and one gate line per threshold with its margin: first
                status within 600 s, at least 0.125 migration pages per second, no call of
                60 s or more, no failed call, every traffic kind answered at least once, no
                publication deadline expiry, no replay release failure, shipper failure or
                item the shipper held back for a full root, no refused claim, at least one
                completed watch claim, each watch's completed claims at least its items
                released before the final status less its in-flight allowance (its
                quiet_ms, the daemon's 45 s publication deadline, and for a shipped watch
                the ship interval and the longest shipper run), and at least one accepted
                socket ingest when any was saved. Exit 4 means the run completed and a gate
                failed. SPARROW_MEASURE_GATE_FIRST_STATUS_SECONDS,
                SPARROW_MEASURE_GATE_RPC_SECONDS, SPARROW_MEASURE_GATE_PAGES_PER_SECOND and
                SPARROW_MEASURE_GATE_INFLIGHT_SECONDS (a ceiling on the in-flight
                allowance) may only tighten their gate.
--rehearse-rollback
                with --with-traffic, after the measurement: rehearse both rollback claim
                findings on scratch copies of the measured copy with the measured image, a
                claim the restored store does not know moved back from .sparrow-inflight/ and
                a claim the newer store published after the backup re-shipped and retired
                (deploy/reconcile-inflight.sh), and fail (FAIL rehearsal) unless both
                reconcile.
USAGE
}

TIME_RECORDS=0 MEASURE_MIGRATION=0 WITH_TRAFFIC=0 REHEARSE=0 IMAGE_OVERRIDE='' WINDOW_FROM='' WINDOW_BEFORE=''
while (($#)); do
    case $1 in
        -h | --help) usage; exit 0 ;;
        --time-records) TIME_RECORDS=1; shift ;;
        --measure-migration) MEASURE_MIGRATION=1; shift ;;
        --with-traffic) WITH_TRAFFIC=1; shift ;;
        --rehearse-rollback) REHEARSE=1; shift ;;
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
RESTORE_MARGIN_GB=${SPARROW_VERIFY_MARGIN_GB:-40}
# Plain decimal of at most nine digits: no octal reading of a leading zero, no arithmetic overflow.
if [[ ! $RESTORE_MARGIN_GB =~ ^[1-9][0-9]{1,8}$ ]] || ((RESTORE_MARGIN_GB < 40)); then
    printf 'verify-backup.sh: SPARROW_VERIFY_MARGIN_GB may only raise 40 (whole GB)\n' >&2
    exit 2
fi
# Seconds between status polls while measuring; progress and free space are logged every minute.
POLL_SECONDS=15
# Seconds the migrations' done/total may stay unchanged after the first status before the run
# fails as stalled. A failing page already ends the run as FAIL migrate, so this catches stops
# that set no failure, such as the daemon pausing writes below its free-space floor. The worker
# retries a failed page every 60 seconds, and a healthy migration page commits well inside a
# poll, so a healthy migration moves every poll; ten retry intervals without movement is a
# stopped migration, and also the switch gate's whole budget.
STALL_SECONDS=600
# The scope limits as systemd reports them once applied (deploy/systemd/sparrow.service). The
# measured daemon also runs at the nice level of the user unit that serves the live store, read
# from systemd below, and SCOPE_LIMITS ends with it.
SCOPE_LIMITS='CPUQuotaPerSecUSec=1s IOWeight=20 MemoryHigh=2147483648 MemoryMax=4294967296'
MEASURE_UNIT=${SPARROW_MEASURE_UNIT:-sparrow.service}
# The switch gate with traffic (docs/deployment.md, "Measure the online migration with intake
# and traffic"). The first status answer within 10 minutes is the gate of the switch itself.
# The page floor, 0.125 pages per second (8 s per page), rejects the failed live backfill of
# 2026-11-07 (over 16 s per page) and is about 38 percent of the schema 69 copy without
# traffic (2,614 pages in 7,914 s, 0.33 pages per second), which leaves writers, served first,
# their share; at the floor a 2,614-page backfill ends within 5.8 hours, inside one night
# before the 04:00 backup. 60 s is the connection lease that cut calls off live. Each may only
# be tightened, which the tests use to make each gate fail.
GATE_FIRST_STATUS=${SPARROW_MEASURE_GATE_FIRST_STATUS_SECONDS:-600}
GATE_RPC_SECONDS=${SPARROW_MEASURE_GATE_RPC_SECONDS:-60}
GATE_PAGES=${SPARROW_MEASURE_GATE_PAGES_PER_SECOND:-0.125}
# A ceiling on each watch's in-flight allowance, which is otherwise derived from the copy's
# configuration; unset leaves it derived, and it can only make the watch gates stricter.
GATE_INFLIGHT=${SPARROW_MEASURE_GATE_INFLIGHT_SECONDS:-}
# The recorded window replayed before the backup's taken_utc, and the traffic cadence. The window
# may only widen, to a day, for a shipper source whose deliveries that hour does not hold.
REPLAY_SECONDS=${SPARROW_MEASURE_REPLAY_SECONDS:-3600} TRAFFIC_PERIOD=30
# The shipper runs this long after each completed run, as sparrow-ship.timer does; and the load
# keeps running at least this long after the first status even when the backfill ends sooner.
# Both may only make the load heavier or longer: the ship interval shorter, the minimum longer.
SHIP_SECONDS=${SPARROW_MEASURE_SHIP_SECONDS:-300}
MIN_TRAFFIC=${SPARROW_MEASURE_MIN_TRAFFIC_SECONDS:-0}
LOAD_HELPER=$(dirname -- "${BASH_SOURCE[0]}")/libexec/sparrow-measure-load
SHIP_HELPER=$(dirname -- "${BASH_SOURCE[0]}")/libexec/sparrow-ship
RECONCILE=$(dirname -- "${BASH_SOURCE[0]}")/reconcile-inflight.sh
((WITH_TRAFFIC == 0 || MEASURE_MIGRATION)) || { printf 'verify-backup.sh: --with-traffic needs --measure-migration\n' >&2; exit 2; }
((REHEARSE == 0 || WITH_TRAFFIC)) || { printf 'verify-backup.sh: --rehearse-rollback needs --with-traffic\n' >&2; exit 2; }
if ((WITH_TRAFFIC)); then
    if [[ ! $GATE_FIRST_STATUS =~ ^[0-9]+$ ]] || ((GATE_FIRST_STATUS > 600)); then
        printf 'verify-backup.sh: SPARROW_MEASURE_GATE_FIRST_STATUS_SECONDS may only lower 600\n' >&2
        exit 2
    fi
    if [[ ! $GATE_RPC_SECONDS =~ ^[0-9]+$ ]] || ((GATE_RPC_SECONDS > 60)); then
        printf 'verify-backup.sh: SPARROW_MEASURE_GATE_RPC_SECONDS may only lower 60\n' >&2
        exit 2
    fi
    if [[ ! $SHIP_SECONDS =~ ^[0-9]+$ ]] || ((SHIP_SECONDS < 1 || SHIP_SECONDS > 300)); then
        printf 'verify-backup.sh: SPARROW_MEASURE_SHIP_SECONDS may only shorten 300, to 1 or more\n' >&2
        exit 2
    fi
    if [[ ! $REPLAY_SECONDS =~ ^[0-9]+$ ]] || ((REPLAY_SECONDS < 3600 || REPLAY_SECONDS > 86400)); then
        printf 'verify-backup.sh: SPARROW_MEASURE_REPLAY_SECONDS may only widen 3600, up to 86400\n' >&2
        exit 2
    fi
    if [[ -n $GATE_INFLIGHT && ! $GATE_INFLIGHT =~ ^[0-9]+$ ]]; then
        printf 'verify-backup.sh: SPARROW_MEASURE_GATE_INFLIGHT_SECONDS must be a whole number of seconds\n' >&2
        exit 2
    fi
    if [[ ! $MIN_TRAFFIC =~ ^[0-9]+$ ]] || ((MIN_TRAFFIC > 300)); then
        printf 'verify-backup.sh: SPARROW_MEASURE_MIN_TRAFFIC_SECONDS must be 0 to 300\n' >&2
        exit 2
    fi
    python3 -I -c 'import sys; v=float(sys.argv[1]); sys.exit(not v >= 0.125)' "$GATE_PAGES" 2>/dev/null \
        || { printf 'verify-backup.sh: SPARROW_MEASURE_GATE_PAGES_PER_SECOND may only raise 0.125\n' >&2; exit 2; }
fi
if ((MEASURE_MIGRATION)); then
    ((TIME_RECORDS == 0)) && [[ -z $WINDOW_FROM ]] \
        || die "--measure-migration measures the migrations only; it takes no --time-records or --window"
    if [[ ! $MIN_FREE_GB =~ ^[0-9]+$ || ! $ABORT_FREE_GB =~ ^[0-9]+$ ]] \
        || ((MIN_FREE_GB < 50 || ABORT_FREE_GB < 40 || ABORT_FREE_GB >= MIN_FREE_GB)); then
        die "the disk guard needs SPARROW_MEASURE_MIN_FREE_GB >= 50 and SPARROW_MEASURE_ABORT_FREE_GB >= 40, below it"
    fi
    command -v systemd-run >/dev/null || die "--measure-migration needs systemd-run for the user scope that limits the daemon"
    # A copy measured at another priority than the unit's does not predict the unit: the
    # 2026-11-07 copy ran at nice 0 while the unit runs at Nice=10.
    unit_load=$(systemctl --user show "$MEASURE_UNIT" -p LoadState --value) \
        || die "systemctl cannot show the user unit $MEASURE_UNIT whose Nice the measurement applies"
    [[ $unit_load == loaded ]] || die "the user unit $MEASURE_UNIT is $unit_load, so its Nice is \
unknown; set SPARROW_MEASURE_UNIT to the user unit that serves the store"
    UNIT_NICE=$(systemctl --user show "$MEASURE_UNIT" -p Nice --value) \
        || die "systemctl cannot show the Nice of the user unit $MEASURE_UNIT"
    [[ $UNIT_NICE =~ ^-?[0-9]+$ ]] || die "the user unit $MEASURE_UNIT reports Nice '$UNIT_NICE', not a nice level"
    # The unit runs under the default time-sharing policy. A caller under SCHED_IDLE, as an
    # agent session can be, would hand its policy to the scope, which then does not predict the
    # unit either, so the daemon is started through chrt --other.
    unit_policy=$(systemctl --user show "$MEASURE_UNIT" -p CPUSchedulingPolicy --value) \
        || die "systemctl cannot show the CPUSchedulingPolicy of the user unit $MEASURE_UNIT"
    [[ $unit_policy == 0 ]] || die "the user unit $MEASURE_UNIT runs under scheduling policy \
$unit_policy, not the time-sharing policy (0) this measurement applies"
    command -v chrt >/dev/null || die "--measure-migration needs chrt to start the daemon under the unit's time-sharing policy"
    SCOPE_LIMITS="$SCOPE_LIMITS Nice=$UNIT_NICE Policy=TS"
fi
# The daemon started here inherits this shell's descriptor limit, not the unit's. The store
# refuses to open below a soft limit of 2048 and bounds its table cache to the rest (#148), so
# the copy runs under the unit's LimitNOFILE=65536, before anything is copied.
ulimit -Sn 65536 2>/dev/null \
    || die "cannot raise the soft open-file limit to 65536 (hard limit $(ulimit -Hn)); the store needs the unit's LimitNOFILE"
BACKUP=$(realpath -e -- "$1") || die "backup $1 does not exist"
[[ -r $BACKUP/SOURCE ]] || die "$BACKUP holds no SOURCE; it is not a backup set"
BACKUP_ID=${BACKUP##*/}
[[ $BACKUP_ID =~ ^[0-9]+$ && ${BACKUP%/*} == */sets ]] || die "$BACKUP is not a backup set \
BACKUP_ROOT/sets/ID"
BACKUP_ROOT=${BACKUP%/sets/*}
[[ -d $BACKUP_ROOT/engine/meta && -r $BACKUP_ROOT/engine/meta/$BACKUP_ID ]] \
    || die "$BACKUP_ROOT/engine holds no backup $BACKUP_ID"
[[ $(sed -n 's/^backup_id=//p' "$BACKUP/SOURCE") == "$BACKUP_ID" ]] \
    || die "$BACKUP/SOURCE does not record backup_id=$BACKUP_ID"

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
# With traffic, also the live ingest socket, the shipper's intake root and state, and the
# producers' own directories, none of which the replay reads: its bytes come from the copy.
# sparrow-backup copies /etc/sparrow, which holds ship.toml, into the set as config/sparrow.
SHIP_CONFIG=$BACKUP/config/sparrow/ship.toml
if ((WITH_TRAFFIC)); then
    value=$(sed -n -e '/^\[/q' -e 's/^ingest_socket_path *= *"\([^"]*\)".*/\1/p' "$SPARROW_CONFIG" | head -n 1)
    [[ -z $value ]] || recorded+=("$value")
    [[ -r $SHIP_CONFIG ]] || die "$SHIP_CONFIG is missing; --with-traffic replays the producers its sources name"
    mapfile -t shipped < <(python3 -I -c 'import sys,tomllib
c=tomllib.load(open(sys.argv[1],"rb"))
print(c["intake_root"]); print(c["state_dir"])
for s in c.get("source",[]): print(s["dir"])' "$SHIP_CONFIG") || die "cannot read $SHIP_CONFIG"
    ((${#shipped[@]} > 2)) || die "$SHIP_CONFIG names no [[source]] to replay"
    recorded+=("${shipped[@]}")
fi
guarded=()
for path in "$live_data" "$live_socket" "$SPARROW_INTAKE" "$BACKUP_ROOT" "${recorded[@]}"; do
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
# The status counts field: pending while the store counters seed, else exact.
status_counts() {
    python3 -I -c 'import json,sys; print(json.load(sys.stdin)["counts"])'
}

expected_store=$(source_value store_id)
[[ -n $expected_store ]] || die "$BACKUP/SOURCE records no store_id"
taken=$(source_value taken_utc)
[[ -n $taken ]] || die "$BACKUP/SOURCE records no taken_utc"
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
# The restore is the image's own: an image without it cannot read an online backup.
[[ $daemon_usage == *"--restore-backup"* ]] || die "$daemon has no --restore-backup (it reports: \
$daemon_usage), so it cannot restore an online backup; choose an image that has it (--image)"
# The writable measurement needs the mode that runs maintenance but opens no watch root.
((MEASURE_MIGRATION == 0)) || [[ $daemon_usage == *"--no-intake"* ]] || die "$daemon has no \
--no-intake mode (it reports: $daemon_usage), so a writable copy would claim its watch roots; \
measure with an image that has the mode (--image)"
# The traffic measurement needs the mode that opens every watch at a private root.
((WITH_TRAFFIC == 0)) || [[ $daemon_usage == *"--private-intake"* ]] || die "$daemon has no \
--private-intake mode (it reports: $daemon_usage), so its watches would open the live roots; \
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
PID='' LOAD_PID='' EXTRA_PIDS=()
# The private intake roots, the copy's own ingest socket and the load helper's work directory.
INTAKE=$SCRATCH/intake INGEST_DIR=$SCRATCH/ingest WORK=$SCRATCH/load
INGEST_SOCKET=$INGEST_DIR/ingest.sock
cleanup() {
    local status=$? pid
    if [[ -n $LOAD_PID ]] && kill -0 "$LOAD_PID" 2>/dev/null; then
        kill -TERM "$LOAD_PID" 2>/dev/null || true
        wait "$LOAD_PID" 2>/dev/null || true
    fi
    for pid in "$PID" "${EXTRA_PIDS[@]}"; do
        if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
            kill -INT "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
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

# Every file the manifest lists is read back from the NAS, and the set holds exactly the files it
# lists, before anything is restored.
command -v b3sum >/dev/null || die "b3sum is missing; it checks the backup against $BACKUP/MANIFEST.b3"
[[ -r $BACKUP/MANIFEST.b3 ]] || fail manifest "$BACKUP holds no MANIFEST.b3"
set_prefix="sets/$BACKUP_ID/"
diff <(sed -E 's/^[0-9a-f]{64}  //' -- "$BACKUP/MANIFEST.b3" | grep -F -- "$set_prefix" | sort) \
    <(cd -- "$BACKUP_ROOT" && find "sets/$BACKUP_ID" -type f ! -name MANIFEST.b3 -printf '%p\n' | sort) \
    >"$SCRATCH/set.diff" \
    || fail manifest "the files of $BACKUP differ from its MANIFEST.b3: $(head -n 1 -- "$SCRATCH/set.diff")"
grep -Eq "^[0-9a-f]{64}  engine/meta/$BACKUP_ID\$" -- "$BACKUP/MANIFEST.b3" \
    || fail manifest "$BACKUP/MANIFEST.b3 does not list engine/meta/$BACKUP_ID"
if ! check=$(cd -- "$BACKUP_ROOT" && b3sum --check --quiet -- "sets/$BACKUP_ID/MANIFEST.b3" 2>&1); then
    fail manifest "${check%%$'\n'*} (against $BACKUP/MANIFEST.b3)"
fi
pass manifest "$(wc -l <"$BACKUP/MANIFEST.b3") files match $BACKUP/MANIFEST.b3, read from $BACKUP_ROOT"

# The restore writes the engine's files into a no-copy-on-write directory ("COW off everywhere");
# the scratch working copy is removed by cleanup however the run ends.
need=$(source_value engine_bytes)
[[ $need =~ ^[0-9]+$ ]] || die "$BACKUP/SOURCE records no engine_bytes"
# --with-traffic restores the set a second time, into the query-only prep copy, while the
# measured copy exists.
copies=1
((WITH_TRAFFIC == 0)) || copies=2
free=$(free_bytes "$SCRATCH") || exit 1
# The scratch filesystem is shared with the live store and with other writers (2026-11-11: a 38 GB
# restore on a filesystem with 86 GB free left the live daemon paused for 19.5 minutes at its
# 4 GiB floor). The restore must leave RESTORE_MARGIN_GB free after the copy, whatever else runs.
margin=$((RESTORE_MARGIN_GB * GB))
printf 'verify-backup: restore margin: %s bytes free, copy needs %s bytes, margin %s GB, %s bytes would remain\n' \
    "$free" "$((copies * need))" "$RESTORE_MARGIN_GB" "$((free - copies * need))"
((free > copies * need + margin)) || die "only $free bytes free under $VERIFY_ROOT; restoring \
backup $BACKUP_ID $copies time(s) needs $((copies * need)) bytes and must leave ${RESTORE_MARGIN_GB} GB free for other writers"
mkdir -- "$DATA"
switch_off_copy_on_write "$DATA"
"$daemon" --restore-backup "$BACKUP_ROOT" "$BACKUP_ID" "$DATA" 2>"$SCRATCH/restore.log" \
    || fail restore "$(tail -n 1 -- "$SCRATCH/restore.log")"
pass restore "backup $BACKUP_ID restored from $BACKUP_ROOT with every file's checksum checked"
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

# Start the image in the background with ARGS, its stderr in LOG; sets STARTED_PID. Every
# daemon it starts opens a scratch copy and is stopped by cleanup however the run ends.
spawn() {
    local log=$1
    shift
    "$daemon" "$@" 2>"$log" >/dev/null &
    STARTED_PID=$!
    EXTRA_PIDS+=("$STARTED_PID")
}
# Wait up to LIMIT seconds for PID's startup line in LOG and a status answer on SOCKET through
# CLI (default the image's), and print the status. Returns 1 when the daemon exits or the time
# runs out.
await_status() {
    local pid=$1 log=$2 socket=$3 deadline=$((SECONDS + $4)) client=${5:-$cli} out
    until grep -q '^sparrowd: store=' -- "$log"; do
        kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)) || return 1
        sleep 1
    done
    until out=$("$client" "$socket" status 2>/dev/null); do
        kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)) || return 1
        sleep 1
    done
    printf '%s\n' "$out"
}
# Stop PID through SIGINT and return its exit status.
halt() {
    local code=0
    kill -INT "$1" 2>/dev/null || true
    wait "$1" || code=$?
    return "$code"
}

if ((WITH_TRAFFIC)); then
    # The private roots and the ingest socket's directory: owned by this user, the socket's
    # directory with this user's group and mode 2750, as the daemon requires.
    mkdir -m 0700 -- "$INTAKE" "$WORK"
    mkdir -- "$INGEST_DIR"
    chmod 2750 -- "$INGEST_DIR"
    require_socket_length "$INGEST_SOCKET"
    for path in "$INTAKE" "$INGEST_DIR" "$WORK"; do
        resolved=$(realpath -e -- "$path") || exit 1
        guard "$path" "$resolved"
    done
    window_start=$((taken_seconds - REPLAY_SECONDS))
    # The window's artifacts, read once from a query-only prep copy, so the measured open stays
    # the copy's first and the prep's reads are not measured.
    prep_started=$SECONDS
    # The prep copy is a second restore of the set, so nothing the prep daemons do reaches the
    # measured copy.
    PREP=$SCRATCH/prep
    mkdir -- "$PREP" "$PREP/store"
    switch_off_copy_on_write "$PREP/store"
    "$daemon" --restore-backup "$BACKUP_ROOT" "$BACKUP_ID" "$PREP/store" 2>"$PREP/restore.log" \
        || fail prep "cannot restore the prep copy: $(tail -n 1 -- "$PREP/restore.log")"
    {
        printf 'query_only = true\nsocket_path = "%s"\ndata_dir = "%s"\n' "$PREP/store/p.sock" "$PREP/store"
        printf 'text_lexicon_dir = "%s"\nmax_concurrent_requests = 4\nmax_rpc_connections = 16\n' "$lexicons"
        printf '%s\n' "${limits[@]}"
    } >"$PREP/server.toml"
    # The backup's own record count, read before anything is replayed by the image the backup
    # recorded, query-only, so nothing migrates or expires: that image's count is the store's
    # count as it was taken. A newer image may upgrade the prep copy and report its counts
    # pending, so the backup's image opens it first. Without that image, or when it too reports
    # pending, the measured image's query-only prep answer counts once it is exact, and
    # otherwise the count is reported pending. The writable copy never counts: its maintenance
    # may run an expiry pass before its first answer.
    BACKUP_RECORDS='' BACKUP_COUNTS=pending BACKUP_COUNT_SOURCE=none
    recorded_image=$(source_value image)
    if [[ $recorded_image == sparrowd-* ]]; then
        recorded_cli_name=$(source_value cli)
        recorded_daemon=$BACKUP/images/$recorded_image
        recorded_cli=$BACKUP/images/${recorded_cli_name:-sparrow-${recorded_image#sparrowd-}}
        if [[ -x $recorded_daemon && -x $recorded_cli ]] \
            && [[ $("$recorded_daemon" 2>&1 || true) == *"--query-only"* ]]; then
            "$recorded_daemon" --config "$PREP/server.toml" --query-only 2>"$PREP/recorded.log" >/dev/null &
            recorded_pid=$!
            EXTRA_PIDS+=("$recorded_pid")
            recorded_status=$(await_status "$recorded_pid" "$PREP/recorded.log" "$PREP/store/p.sock" 3600 "$recorded_cli") \
                || fail prep "the backup's image $recorded_daemon did not answer status query-only: $(tail -n 5 -- "$PREP/recorded.log")"
            halt "$recorded_pid" || fail prep "the backup's image exited non-zero after SIGINT"
            if [[ $(status_counts <<<"$recorded_status") == exact ]]; then
                BACKUP_RECORDS=$(json_field live_records <<<"$recorded_status") || exit 1
                BACKUP_COUNTS=exact BACKUP_COUNT_SOURCE=backup-image
            fi
        else
            printf 'verify-backup: the backup image %s is missing or has no --query-only; its record count comes from the query-only prep status when exact, otherwise it is pending\n' "$recorded_daemon"
        fi
    fi
    spawn "$PREP/daemon.log" --config "$PREP/server.toml" --query-only
    prep_pid=$STARTED_PID
    await_status "$prep_pid" "$PREP/daemon.log" "$PREP/store/p.sock" 3600 >"$WORK/prep-status.json" \
        || fail prep "the query-only prep copy did not answer status: $(tail -n 5 -- "$PREP/daemon.log")"
    if [[ $BACKUP_COUNTS != exact && $(status_counts <"$WORK/prep-status.json") == exact ]]; then
        BACKUP_RECORDS=$(json_field live_records <"$WORK/prep-status.json") || exit 1
        BACKUP_COUNTS=exact BACKUP_COUNT_SOURCE=prep-query-only
    fi
    "$LOAD_HELPER" prepare-replay --cli "$cli" --socket "$PREP/store/p.sock" --store "$expected_store" \
        --work "$WORK" --intake "$INTAKE" --status "$WORK/prep-status.json" --ship-config "$SHIP_CONFIG" \
        --window-from "$window_start" --window-before "$taken_seconds" \
        || fail prep "the window's intake cannot be replayed (see the line above)"
    halt "$prep_pid" || fail prep "the prep daemon exited non-zero after SIGINT"
    rm -rf -- "$PREP"
    printf 'verify-backup: MEASURE prep_seconds=%s\n' $((SECONDS - prep_started))
    printf 'ingest_socket_path = "%s"\ningest_socket_gid = %s\nmax_ingest_connections = 16\n' \
        "$INGEST_SOCKET" "$(id -g)" >>"$CONFIG"
fi

# Seconds elapsed since START (date +%s%N), to the millisecond.
elapsed() {
    local now
    now=$(date +%s%N) || return 1
    printf '%d.%03d' $(((now - $1) / 1000000000)) $(((now - $1) / 1000000 % 1000))
}
# The watch roots a status document on stdin reports outside the private intake directory, and
# the ingest socket when it is not the copy's own; empty when everything is private.
private_violations() {
    python3 -I -c 'import json,os,sys
s=json.load(sys.stdin); intake=os.path.realpath(sys.argv[1])
bad=[w["root"] for w in s["consume_watches"] if os.path.dirname(os.path.realpath(w["root"]))!=intake]
sock=(s.get("ingest_socket") or {}).get("path")
if sock!=sys.argv[2]: bad.append("ingest-socket=%s" % sock)
print(" ".join(bad))' "$INTAKE" "$INGEST_SOCKET"
}
# "SECONDS RATE" for PAGES over the run seconds from BEGIN to END.
pages_rate() {
    python3 -I -c 'import sys; s=float(sys.argv[2])-float(sys.argv[3])
print("%.3f %.4f" % (s, int(sys.argv[1])/s if s > 0 else 0.0))' "$@"
}
# The number of stored watch roots a status document on stdin reports open.
open_watches() {
    json_field consume_watches | python3 -I -c 'import json,sys; print(sum(w["directories"] != "0" and w["directories"] != 0 for w in json.load(sys.stdin)))'
}

# A rehearsal configuration on stdout: the scratch configuration's budgets with DATA, SOCKET,
# no ingest socket, max_records MAX, and query_only when QUERY_ONLY is set.
rehearsal_config() {
    local data=$1 socket=$2 max=$3 query_only=${4:-}
    [[ -z $query_only ]] || printf 'query_only = true\n'
    printf 'socket_path = "%s"\ndata_dir = "%s"\ntext_lexicon_dir = "%s"\n' "$socket" "$data" "$lexicons"
    printf 'max_concurrent_requests = 4\nmax_rpc_connections = 16\n'
    printf '%s\n' "${limits[@]}" | sed "s/^max_records = .*/max_records = $max/"
}
# Wait up to LIMIT seconds for an entry other than .lock and SKIP in ROOT/.sparrow-inflight/,
# and print its name.
await_inflight() {
    local root=$1 skip=$2 deadline=$((SECONDS + $3)) entry
    while ((SECONDS < deadline)); do
        for entry in "$root"/.sparrow-inflight/*; do
            entry=${entry##*/}
            [[ $entry == '*' || $entry == "$skip" ]] || { printf '%s\n' "$entry"; return 0; }
        done
        sleep 1
    done
    return 1
}
# Write LINES one-line JSON records to NAME in ROOT by a rename, as the shipper feeds a root.
place_batch() {
    local root=$1 name=$2 lines=$3 line
    for ((line = 1; line <= lines; line++)); do printf '{"rehearsal":"%s","line":%d}\n' "$name" "$line"; done \
        >"$SCRATCH/rehearsal/$name"
    mv -- "$SCRATCH/rehearsal/$name" "$root/$name"
}
# Rehearse the rollback's claim reconciliation (docs/deployment.md, "Claims after a rollback")
# on the measured copy, restored from the set, with the measured image in every role and
# every watch rebound below the rehearsal's own private directory. A watch put on the copy
# feeds three record batches through it; max_records decides what each daemon can publish.
#  1. Daemon A (max_records 2) claims batch-1 (3 records) and cannot publish it; it stops.
#  2. The "restored backup": a scratch copy of A's stopped store, which journals claim C1. It
#     stands in for the store `--restore-backup` makes from a set taken at that point; the
#     daemon takes online backups only to a network backup_root, so a scratch run cannot.
#  3. The newer daemon B (max_records 4) publishes and removes batch-1, then claims batch-2
#     (5 records) and cannot publish it: C2 stays in .sparrow-inflight/; it stops.
#  4. The restored daemon on the backup refuses to start: C2 is unknown to it (finding 1).
#  5. reconcile-inflight.sh inflight, with both stores query-only, moves C2 back to its root
#     as batch-2.jsonl, the same inode.
#  6. The restored daemon starts; C1's private source is missing (finding 2).
#     reconcile-inflight.sh missing re-ships C1's bytes from the newer store and retires C1.
rehearse_rollback() {
    local rehearsal=$SCRATCH/rehearsal intake=$SCRATCH/rehearsal/intake root name pid
    local claimed newer_claim inode out restored newer newer_pid
    mkdir -m 0700 -- "$rehearsal" "$intake"
    root=$intake/measure-rehearsal
    while read -r name; do
        mkdir -p -- "$intake/$name"
    done < <(python3 -I -c 'import json,os,sys
for w in json.load(sys.stdin)["consume_watches"]: print(os.path.basename(w["root"]))' <<<"$status")
    mkdir -p -- "$root"
    rehearsal_config "$DATA" "$DATA/r.sock" 2 >"$rehearsal/a.toml"
    spawn "$rehearsal/a.log" --config "$rehearsal/a.toml" --private-intake "$intake"
    pid=$STARTED_PID
    await_status "$pid" "$rehearsal/a.log" "$DATA/r.sock" 600 >/dev/null \
        || fail rehearsal "daemon A did not answer: $(tail -n 3 -- "$rehearsal/a.log")"
    # The put names a configured root outside the private directory, as every catalog root
    # is; the daemon rebinds it to $root.
    printf '%s\n' 'id = "measure-rehearsal"' "root = \"$rehearsal/configured/measure-rehearsal\"" 'producer_contract = "finished_files"' \
        'source = "measure-rehearsal"' 'format = "jsonl"' 'quiet_ms = 1000' 'max_directories = 16' \
        'max_candidates = 64' >"$rehearsal/watch.toml"
    out=$("$cli" "$DATA/r.sock" watch put measure-rehearsal-put 0 "$rehearsal/watch.toml" 2>&1) \
        || fail rehearsal "watch put refused: $out"
    place_batch "$root" batch-1.jsonl 3
    claimed=$(await_inflight "$root" '' 300) || fail rehearsal "daemon A never claimed batch-1.jsonl"
    halt "$pid" || fail rehearsal "daemon A exited non-zero after SIGINT"

    restored=$rehearsal/backup
    mkdir -- "$restored"
    match_copy_on_write "$DATA" "$restored"
    cp -a --reflink=always -- "$DATA/." "$restored/" || fail rehearsal "cannot reflink the rehearsal backup"

    rehearsal_config "$DATA" "$DATA/r.sock" 4 >"$rehearsal/b.toml"
    spawn "$rehearsal/b.log" --config "$rehearsal/b.toml" --private-intake "$intake"
    pid=$STARTED_PID
    await_status "$pid" "$rehearsal/b.log" "$DATA/r.sock" 600 >/dev/null \
        || fail rehearsal "the newer daemon did not answer: $(tail -n 3 -- "$rehearsal/b.log")"
    for ((name = 0; name < 300; name++)); do
        [[ -e $root/.sparrow-inflight/$claimed ]] || break
        sleep 1
    done
    [[ ! -e $root/.sparrow-inflight/$claimed ]] || fail rehearsal "the newer daemon never published $claimed"
    place_batch "$root" batch-2.jsonl 5
    newer_claim=$(await_inflight "$root" "$claimed" 300) || fail rehearsal "the newer daemon never claimed batch-2.jsonl"
    halt "$pid" || fail rehearsal "the newer daemon exited non-zero after SIGINT"
    newer=$DATA

    rehearsal_config "$restored" "$restored/r.sock" 8 >"$rehearsal/c.toml"
    spawn "$rehearsal/c.log" --config "$rehearsal/c.toml" --private-intake "$intake"
    pid=$STARTED_PID
    for ((name = 0; name < 600; name++)); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    ! kill -0 "$pid" 2>/dev/null || fail rehearsal "the restored daemon started over the unknown claim $newer_claim"
    wait "$pid" || true
    grep -q 'unrecognized private claim entry' -- "$rehearsal/c.log" \
        || fail rehearsal "the restored daemon stopped without finding 1's refusal: $(tail -n 3 -- "$rehearsal/c.log")"
    pass rehearsal-refusal "the restored store refuses to start over $newer_claim, which only the newer store journals"

    inode=$(stat -c %i -- "$root/.sparrow-inflight/$newer_claim") || exit 1
    rehearsal_config "$restored" "$restored/r.sock" 8 query >"$rehearsal/rq.toml"
    rehearsal_config "$newer" "$DATA/n.sock" 8 query >"$rehearsal/nq.toml"
    spawn "$rehearsal/rq.log" --config "$rehearsal/rq.toml" --query-only
    pid=$STARTED_PID
    spawn "$rehearsal/nq.log" --config "$rehearsal/nq.toml" --query-only
    newer_pid=$STARTED_PID
    await_status "$pid" "$rehearsal/rq.log" "$restored/r.sock" 600 >/dev/null \
        || fail rehearsal "the restored query-only daemon did not answer"
    await_status "$newer_pid" "$rehearsal/nq.log" "$DATA/n.sock" 600 >/dev/null \
        || fail rehearsal "the newer query-only daemon did not answer"
    out=$("$RECONCILE" inflight --restored "$cli" "$restored/r.sock" --newer "$cli" "$DATA/n.sock" \
        --private-intake "$intake" 2>&1) || fail rehearsal "reconcile-inflight.sh inflight failed: $out"
    printf '%s\n' "$out" | sed 's/^/verify-backup: rehearsal: /'
    [[ $out == *"inflight moved=1 kept=0"* && ! -e $root/.sparrow-inflight/$newer_claim ]] \
        && [[ $(stat -c %i -- "$root/batch-2.jsonl") == "$inode" ]] \
        || fail rehearsal "$newer_claim was not moved back to batch-2.jsonl with its inode $inode"
    pass rehearsal-inflight "$newer_claim is back in its root as batch-2.jsonl, inode $inode unchanged"
    halt "$pid" || fail rehearsal "the restored query-only daemon exited non-zero"

    spawn "$rehearsal/c2.log" --config "$rehearsal/c.toml" --private-intake "$intake"
    pid=$STARTED_PID
    await_status "$pid" "$rehearsal/c2.log" "$restored/r.sock" 600 >/dev/null \
        || fail rehearsal "the restored daemon did not start after reconciliation: $(tail -n 3 -- "$rehearsal/c2.log")"
    for ((name = 0; name < 300; name++)); do
        out=$("$cli" "$restored/r.sock" status) || fail rehearsal "the restored daemon stopped answering"
        [[ $out == *"claim $claimed: unpublished claimed source is missing"* ]] && break
        sleep 1
    done
    [[ $out == *"claim $claimed: unpublished claimed source is missing"* ]] \
        || fail rehearsal "the restored daemon never reported $claimed as missing its source"
    out=$("$RECONCILE" missing --restored "$cli" "$restored/r.sock" --newer "$cli" "$DATA/n.sock" \
        --private-intake "$intake" 2>&1) || fail rehearsal "reconcile-inflight.sh missing failed: $out"
    printf '%s\n' "$out" | sed 's/^/verify-backup: rehearsal: /'
    [[ $out == *"missing reshipped=1 unretired=0"* ]] || fail rehearsal "$claimed was not re-shipped and retired"
    out=$("$cli" "$restored/r.sock" status) || fail rehearsal "the restored daemon stopped answering"
    python3 -I -c 'import json,sys
w=[w for w in json.load(sys.stdin)["consume_watches"] if w["id"]=="measure-rehearsal"][0]
sys.exit(not (sys.argv[1] in json.dumps(w["retired_claims"]) and "retired-unrecoverable" in json.dumps(w["retired_claims"])))' \
        "$claimed" <<<"$out" || fail rehearsal "status does not list $claimed as retired-unrecoverable"
    pass rehearsal-missing "$claimed, published by the newer store after the backup, is re-shipped as batch-1.reshipped.jsonl and retired-unrecoverable"
    halt "$pid" || fail rehearsal "the restored daemon exited non-zero after SIGINT"
    halt "$newer_pid" || fail rehearsal "the newer query-only daemon exited non-zero"
}

if ((MEASURE_MIGRATION)); then
    image_sha=$(sha256sum -- "$daemon") || exit 1
    printf 'verify-backup: MEASURE image=%s sha256=%s\n' "$daemon" "${image_sha%% *}"
    started=$(date +%s%N) || exit 1
    printf 'verify-backup: MEASURE start_utc=%s\n' "$(date -u -d "@$((started / 1000000000))" +%Y-%m-%dT%H:%M:%SZ)"
    mode_args=(--no-intake)
    ((WITH_TRAFFIC == 0)) || mode_args=(--private-intake "$INTAKE")
    # A scope runs the daemon as this process (systemd-run and chrt exec it), so PID is the
    # daemon's.
    # A scope unit has no Nice property: systemd-run sets the nice level itself before it execs
    # the daemon, whose threads inherit it.
    systemd-run --user --scope --quiet --collect --unit="sparrow-${SCRATCH##*/}" \
        --nice="$UNIT_NICE" -p CPUQuota=100% -p IOWeight=20 -p MemoryHigh=2G -p MemoryMax=4G \
        -- chrt --other 0 "$daemon" --config "$CONFIG" "${mode_args[@]}" 2>"$LOG" >/dev/null &
    PID=$!
    # Each migration's newest page count from the daemon's lines, and the run time at which its
    # completion line was first seen; a poll's granularity bounds each timing.
    declare -A PAGES=() DONE_AT=()
    DONE_ORDER=()
    track_pages() {
        local name kind pages now
        now=$(elapsed "$started") || return 1
        while read -r name kind pages; do
            PAGES[$name]=$pages
            if [[ $kind == complete && -z ${DONE_AT[$name]:-} ]]; then
                DONE_AT[$name]=$now
                DONE_ORDER+=("$name")
            fi
        done < <(sed -n 's/^sparrowd: migration \([^ ]*\) \(progress\|complete\) pages=\([0-9]*\).*/\1 \2 \3/p' -- "$LOG")
    }
    # No readiness ceiling: how long the first open takes is what this mode measures.
    first='' first_at=0 completed='' next_minute=$((SECONDS + 60)) failures=0 progress='status=unanswered' moved=0
    while :; do
        kill -0 "$PID" 2>/dev/null || fail open "the daemon exited (see its log below)"
        # The load helper runs for the whole backfill; one that died ends the run now.
        if [[ -n $LOAD_PID ]] && ! kill -0 "$LOAD_PID" 2>/dev/null; then
            load_status=0
            wait "$LOAD_PID" || load_status=$?
            LOAD_PID=''
            fail load "the load helper exited with status $load_status before the backfill completed; see its lines above"
        fi
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
            first_at=$SECONDS
            printf 'verify-backup: MEASURE first_status_seconds=%s\n' "$first"
            store=$(json_field store_id <<<"$status") || exit 1
            [[ $store == "$expected_store" ]] || fail store-id "the copy is $store, but SOURCE records $expected_store"
            mode=$(json_field query_only <<<"$status") || exit 1
            if ((WITH_TRAFFIC)); then
                # Every watch the daemon reports must consume a private root, and the ingest
                # socket must be the copy's own, before any intake is replayed.
                outside=$(private_violations <<<"$status") || exit 1
                if [[ $mode != false ]] || ! grep -q 'query_only=false intake=private' -- "$LOG" || [[ -n $outside ]]; then
                    fail private-intake "the daemon does not report private-intake mode, or these are not private: ${outside:-none}"
                fi
                pass private-intake "$store consumes every watch root below $INTAKE and serves ingest on $INGEST_SOCKET only"
                printf 'verify-backup: MEASURE backup_live_records=%s counts=%s source=%s\n' \
                    "${BACKUP_RECORDS:-pending}" "$BACKUP_COUNTS" "$BACKUP_COUNT_SOURCE"
                # The watches' completed claims count from here.
                printf '%s\n' "$status" >"$WORK/first-status.json"
                load_since=$(date +%s.%N) || exit 1
                "$LOAD_HELPER" run --work "$WORK" --intake "$INTAKE" --cli "$cli" --socket "$SOCKET" \
                    --ingest-socket "$INGEST_SOCKET" --store "$store" --ship "$SHIP_HELPER" \
                    --summary "$WORK/summary.json" --window "$REPLAY_SECONDS" --period "$TRAFFIC_PERIOD" \
                    --ship-interval "$SHIP_SECONDS" &
                LOAD_PID=$!
            else
                opened=$(open_watches <<<"$status") || exit 1
                if [[ $mode != false ]] || ! grep -q 'query_only=false intake=false' -- "$LOG" || ((opened)); then
                    fail no-intake "the daemon does not report no-intake mode, or $opened stored watch roots are open"
                fi
                pass no-intake "$store is writable with no intake; no stored watch root is open"
            fi
            scope="sparrow-${SCRATCH##*/}.scope"
            applied=$(systemctl --user show "$scope" -p CPUQuotaPerSecUSec -p IOWeight -p MemoryHigh \
                -p MemoryMax | sort | paste -sd ' ') || fail scope "systemctl cannot show $scope"
            nice=$(ps -o ni= -p "$PID") || fail scope "cannot read the nice level of the daemon $PID"
            policy=$(ps -o cls= -p "$PID") || fail scope "cannot read the scheduling policy of the daemon $PID"
            applied="$applied Nice=${nice//[[:space:]]/} Policy=${policy//[[:space:]]/}"
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
        track_pages
        [[ ${state[0]} != failed ]] || fail migrate "${state[2]}"
        if [[ ${state[0]} == complete ]]; then
            # The backfill ends at the first poll that sees every migration complete.
            if [[ -z $completed ]]; then
                completed=$(elapsed "$started") || exit 1
                completed_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
            fi
            ((SECONDS - first_at < MIN_TRAFFIC)) || break
        fi
        sleep "$POLL_SECONDS"
    done
    printf 'verify-backup: MEASURE backfill_seconds=%s\n' "$completed"
    printf 'verify-backup: MEASURE end_utc=%s\n' "$completed_utc"
    ((WITH_TRAFFIC)) || printf 'verify-backup: MEASURE live_records=%s counts=%s\n' "$(json_field live_records <<<"$status")" "$(status_counts <<<"$status")"
    pass migrate "every migration is complete with no failure: $progress"
    gate_failed=0
    if ((WITH_TRAFFIC)); then
        backfill=$completed
        kill -TERM "$LOAD_PID" 2>/dev/null || true
        wait "$LOAD_PID" || fail load "the load helper failed; see its lines above"
        LOAD_PID=''
        track_pages
        # Pages per second per migration, each from the previous completion (or the start) to
        # its own, and overall, every page over the whole backfill. Migrations first seen
        # complete at the same poll share that interval: each line gives the group's pages
        # over it and names the migrations that shared the poll.
        begin=0 total_pages=0 index=0
        while ((index < ${#DONE_ORDER[@]})); do
            group=("${DONE_ORDER[index]}")
            while ((index + ${#group[@]} < ${#DONE_ORDER[@]})) \
                && [[ ${DONE_AT[${DONE_ORDER[index + ${#group[@]}]}]} == "${DONE_AT[${group[0]}]}" ]]; do
                group+=("${DONE_ORDER[index + ${#group[@]}]}")
            done
            group_pages=0
            for name in "${group[@]}"; do group_pages=$((group_pages + PAGES[$name])); done
            read -r seconds rate < <(pages_rate "$group_pages" "${DONE_AT[${group[0]}]}" "$begin")
            shared=''
            ((${#group[@]} == 1)) || shared=" shared_poll=$(IFS=,; printf '%s' "${group[*]}") shared_pages=$group_pages"
            for name in "${group[@]}"; do
                printf 'verify-backup: MEASURE pages migration=%s pages=%s seconds=%s pages_per_second=%s%s\n' \
                    "$name" "${PAGES[$name]}" "$seconds" "$rate" "$shared"
            done
            begin=${DONE_AT[${group[0]}]}
            index=$((index + ${#group[@]}))
        done
        for name in "${!PAGES[@]}"; do total_pages=$((total_pages + PAGES[$name])); done
        read -r _ rate < <(pages_rate "$total_pages" "$backfill" 0)
        printf 'verify-backup: MEASURE pages overall pages=%s seconds=%s pages_per_second=%s\n' \
            "$total_pages" "$backfill" "$rate"
        # What the replay added: every record received since the load started, as the copy
        # holds none received later than the backup's taken_utc before it. Then the copy's
        # count at the end, replay included, and the claims its watches completed.
        replay_added=$("$LOAD_HELPER" count-records --cli "$cli" --socket "$SOCKET" --store "$store" \
            --since "$load_since") || fail load "counting the records the replay added failed"
        claims_at=$(date +%s.%N) || exit 1
        status=$("$cli" "$SOCKET" status) || fail status "status did not answer after the load stopped"
        printf '%s\n' "$status" >"$WORK/final-status.json"
        printf 'verify-backup: MEASURE replay_added=%s since_utc=%s\n' "$replay_added" \
            "$(date -u -d "@${load_since%.*}" +%Y-%m-%dT%H:%M:%SZ)"
        printf 'verify-backup: MEASURE live_records=%s counts=%s\n' "$(json_field live_records <<<"$status")" "$(status_counts <<<"$status")"
        claims=$(python3 -I -c 'import json,sys
first={w["id"]: int(w["completed_claims"]) for w in json.load(open(sys.argv[1]))["consume_watches"]}
print(sum(int(w["completed_claims"]) - first.get(w["id"], 0) for w in json.load(sys.stdin)["consume_watches"]))' \
            "$WORK/first-status.json" <<<"$status") || exit 1
        printf 'verify-backup: MEASURE watch_claims_completed=%s\n' "$claims"
        expiries=$(grep -c 'consume publication deadline expired' -- "$LOG" || true)
        printf 'verify-backup: MEASURE publication_deadline_expiries=%s\n' "$expiries"
        refused=$(grep -c '^sparrowd: watch=.* reservation refused for ' -- "$LOG" || true)
        printf 'verify-backup: MEASURE watch_claims_refused=%s\n' "$refused"
        "$LOAD_HELPER" gates --summary "$WORK/summary.json" --first "$first" \
            --first-limit "$GATE_FIRST_STATUS" --pages "$total_pages" --backfill "$backfill" \
            --pages-floor "$GATE_PAGES" --rpc-limit "$GATE_RPC_SECONDS" --expiries "$expiries" \
            --refused "$refused" --first-status "$WORK/first-status.json" \
            --final-status "$WORK/final-status.json" --claims-at "$claims_at" \
            --ship-interval "$SHIP_SECONDS" --inflight-limit "${GATE_INFLIGHT:--1}" || gate_failed=1
    fi
    kill -INT "$PID"
    stopped=0
    wait "$PID" || stopped=$?
    PID=''
    ((stopped == 0)) || fail shutdown "the daemon exited with status $stopped after SIGINT"
    # Retention runs in a writable copy, so its closing count need not equal SOURCE's.
    grep -Eq "^sparrowd: closed store=$store unique_payload_chunks=[0-9]+\$" -- "$LOG" \
        || fail shutdown "the daemon did not log its closing count"
    pass shutdown "the daemon stopped cleanly; $SCRATCH is removed when the run ends"
    ((REHEARSE == 0)) || rehearse_rollback
    if ((gate_failed)); then
        printf 'verify-backup: migration of %s measured; the switch gate FAILED\n' "$BACKUP" >&2
        exit 4
    fi
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
    sleep 1
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

counts=$(status_counts <<<"$status") || exit 1
[[ $counts == exact || $counts == pending ]] || fail status "status reports counts=$counts, neither exact nor pending"

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
        rm -f -- "$chunk" # bytes refuses to overwrite an existing file
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
grep -Eq "^sparrowd: closed store=$store unique_payload_chunks=([0-9]+|pending)\$" -- "$LOG" \
    || fail shutdown "the daemon did not log its closing count"
pass shutdown "the daemon stopped cleanly; removing $SCRATCH"
printf 'verify-backup: %s verified\n' "$BACKUP"
