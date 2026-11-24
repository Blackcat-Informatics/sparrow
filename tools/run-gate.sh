#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

set -euo pipefail

refuse() {
    printf 'run-gate: %s\n' "$1" >&2
    exit 2
}

if (( $# < 3 )) || [[ $2 != -- || -z $1 || -z $3 ]]; then
    refuse 'usage: tools/run-gate.sh LOG -- COMMAND [ARG ...]'
fi

log_arg=$1
shift 2
directory=$(pwd -P)
head=$(git rev-parse --verify HEAD)
main=$("$(dirname -- "${BASH_SOURCE[0]}")/canonical-checkout.sh" "$directory")
stage=$main/.stage
[[ -d $stage && ! -L $stage ]] || refuse "create the canonical checkout's .stage directory at $stage"

log_dir=$(realpath -e -- "$(dirname -- "$log_arg")") ||
    refuse "the log parent must be an existing directory under $stage"
case "$log_dir" in
    "$stage"|"$stage"/*) ;;
    *) refuse "the log must be under $stage" ;;
esac
log=$log_dir/$(basename -- "$log_arg")
[[ ! -e $log && ! -L $log ]] || refuse "choose a new log path: $log"
lock=$stage/cargo.lock
[[ ! -L $lock && ( ! -e $lock || -f $lock ) ]] ||
    refuse "the gate lock must be a regular file: $lock"

exec {lock_fd}>>"$lock"
flock "$lock_fd" || refuse "could not acquire the gate lock: $lock"
# Keep each run's evidence in a new file, including when there is a concurrent caller.
set -o noclobber
exec {log_fd}>"$log"
set +o noclobber

{
    printf 'Directory %s\nCommit %s\nDate %s\nCommand' "$directory" "$head" "$(date -u +%FT%TZ)"
    printf ' %q' "$@"
    printf '\n'
} >&"$log_fd"

status=0
(
    exec {lock_fd}>&- {log_fd}>&-
    "$@"
) >&"$log_fd" 2>&1 || status=$?

# Separate the verdict even when the command's last output has no newline.
printf '\nEXIT=%s\n' "$status" >&"$log_fd"
printf 'EXIT=%s\n' "$status"
exit "$status"
