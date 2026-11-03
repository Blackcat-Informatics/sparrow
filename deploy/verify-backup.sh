#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Verify a closed-store backup end to end (RECOVERY.md, "Verify a backup"). The backup's store is
# reflinked into a scratch directory below SPARROW_VERIFY (default /SPARROW/verify), and the image
# the backup recorded opens that copy in query-only mode, so no watch root is opened, no file is
# claimed and nothing is retired. The script checks the store identity, status, the live record
# count, a records window over the backup's newest hour and one artifact's bytes against its
# catalog digest, then stops the daemon with SIGINT and removes the copy. It prints one line per
# check and exits 1 at the first failed one. It never starts a daemon on the live store, socket or
# intake: it refuses a scratch path that resolves under /SPARROW/store or SPARROW_INTAKE.
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/verify-backup.sh [--time-records] [--image DAEMON] BACKUP

Copy BACKUP/store into a scratch directory below SPARROW_VERIFY (default /SPARROW/verify), open
the copy query-only with the image BACKUP/SOURCE records, and check it. A backup counts as
verified only once this exits 0.

--time-records  after the checks, print five elapsed times of the one-hour records window
--image DAEMON  open the copy with DAEMON instead (its CLI is the sibling sparrow or
                sparrow-SUFFIX); a newer image may upgrade the disposable copy as it opens it
USAGE
}

TIME_RECORDS=0 IMAGE_OVERRIDE=''
while (($#)); do
    case $1 in
        -h | --help) usage; exit 0 ;;
        --time-records) TIME_RECORDS=1; shift ;;
        --image) IMAGE_OVERRIDE=${2:?--image needs a daemon image}; shift 2 ;;
        -*) printf 'verify-backup.sh: unknown option %s (see --help)\n' "$1" >&2; exit 2 ;;
        *) break ;;
    esac
done
(($# == 1)) || { usage >&2; exit 2; }
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

LIVE_STORE=/SPARROW/store
VERIFY_ROOT=${SPARROW_VERIFY:-/SPARROW/verify}
BACKUP=$(realpath -e -- "$1") || die "backup $1 does not exist"
[[ -r $BACKUP/SOURCE && -d $BACKUP/store ]] || die "$BACKUP holds no SOURCE and store; it is not a backup"

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
expected_records=$(source_value live_records)
taken=$(source_value taken_utc)
[[ -n $taken ]] || die "$BACKUP/SOURCE records no taken_utc"
# sparrow-backup writes a compact stamp (20261103T014038Z); older backups an RFC 3339 instant.
if [[ $taken =~ ^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})Z$ ]]; then
    taken="${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]}T${BASH_REMATCH[4]}:${BASH_REMATCH[5]}:${BASH_REMATCH[6]}Z"
fi
taken_seconds=$(date -u -d "$taken" +%s) || die "$BACKUP/SOURCE taken_utc $taken is not a time"
window_from=$(date -u -d "@$((taken_seconds - 3600))" +%Y-%m-%dT%H:%M:%SZ) || exit 1
window_before=$(date -u -d "@$((taken_seconds + 1))" +%Y-%m-%dT%H:%M:%SZ) || exit 1

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
    rm -rf -- "$SCRATCH"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Every path the daemon will own resolves outside the live store, the intake and the backup.
live=$(realpath -m -- "$LIVE_STORE") || exit 1
intake=$(realpath -m -- "$SPARROW_INTAKE") || exit 1
for path in "$DATA" "$SOCKET"; do
    resolved=$(realpath -m -- "$path") || exit 1
    for guarded in "$live" "$intake" "$BACKUP"; do
        [[ $resolved != "$guarded" && $resolved != "$guarded"/* ]] \
            || die "scratch path $path resolves to $resolved, under $guarded; refusing to start a daemon there"
    done
done
require_socket_length "$SOCKET"

cp -a --reflink=always -- "$BACKUP/store" "$DATA" \
    || die "cannot reflink $BACKUP/store below $VERIFY_ROOT; it must be on the backup's btrfs filesystem"
{
    printf 'query_only = true\n'
    printf 'socket_path = "%s"\ndata_dir = "%s"\ntext_lexicon_dir = "%s"\n' "$SOCKET" "$DATA" "$lexicons"
    printf 'max_concurrent_requests = 4\nmax_rpc_connections = 16\n'
    printf '%s\n' "${limits[@]}"
} >"$CONFIG"

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
watches=$(json_field consume_watches <<<"$status") || exit 1
opened=$(python3 -I -c 'import json,sys; print(sum(w["directories"] != "0" and w["directories"] != 0 for w in json.load(sys.stdin)))' <<<"$watches") || exit 1
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

records=$(json_field live_records <<<"$status") || exit 1
unrecorded=0
if [[ -z $expected_records ]]; then
    unrecorded=1
    printf 'verify-backup: SKIP live-records: %s live; SOURCE records no live_records, so this backup predates the count\n' "$records"
elif ((records >= expected_records)); then
    pass live-records "$records live, SOURCE recorded $expected_records"
else
    fail live-records "$records live, fewer than the $expected_records SOURCE recorded"
fi

window=(records --received-from "$window_from" --received-before "$window_before")
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

if ((TIME_RECORDS)); then
    printf 'verify-backup: timing: sparrow SOCKET %s\n' "${window[*]}"
    for run in 1 2 3 4 5; do
        started=$(date +%s%N) || exit 1
        "$cli" "$SOCKET" "${window[@]}" >/dev/null || fail time-records "run $run failed"
        finished=$(date +%s%N) || exit 1
        elapsed=$((finished - started))
        printf 'verify-backup: time-records run %d: %d.%03d s\n' "$run" $((elapsed / 1000000000)) $((elapsed / 1000000 % 1000))
    done
fi

kill -INT "$PID"
stopped=0
wait "$PID" || stopped=$?
PID=''
((stopped == 0)) || fail shutdown "the daemon exited with status $stopped after SIGINT"
pass shutdown "the daemon stopped cleanly; removing $SCRATCH"
if ((unrecorded)); then
    printf 'verify-backup: NOT VERIFIED: SOURCE records no live_records; take a new backup with sparrow-backup\n' >&2
    exit 1
fi
printf 'verify-backup: %s verified\n' "$BACKUP"
