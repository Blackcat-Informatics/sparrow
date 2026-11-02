#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Take the named pre-upgrade backup that start.sh and restart.sh require before an image that
# raises the schema (README.md, "Upgrades and rollback"). It runs one closed-store backup now and
# renames that run's own backup to SPARROW_BACKUPS/pre-upgrade-CURRENT, where CURRENT is the image
# the links name. It stops at the first failure, refuses an existing pre-upgrade-CURRENT, and
# accepts only a backup whose taken_utc is from this run and whose image= is CURRENT.
#
# Run as root. In user scope (a test instance, deploy/test-instance.sh) it runs the backup helper
# directly with the test instance's paths instead of starting sparrow-backup.service.
set -euo pipefail
umask 077

if (($# >= 1)) && [[ $1 == -h || $1 == --help ]]; then
    cat <<'USAGE'
Usage: deploy/pre-upgrade-backup.sh

Back up the store as the current image left it, as SPARROW_BACKUPS/pre-upgrade-CURRENT.
Refuses if that name exists: retire the old copy by renaming it first.
USAGE
    exit 0
fi
(($# == 0)) || { printf 'pre-upgrade-backup.sh: takes no arguments (see --help)\n' >&2; exit 2; }
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

[[ $SPARROW_SCOPE == user || $EUID -eq 0 ]] || die "run as root (sudo deploy/pre-upgrade-backup.sh)"
current=$(readlink -- "$LIBEXEC/sparrowd") || die "no image link $LIBEXEC/sparrowd; there is no current image to back up"
target=$SPARROW_BACKUPS/pre-upgrade-$current
if [[ -e $target ]]; then
    taken=$(sed -n 's/^taken_utc=//p' "$target/SOURCE" 2>/dev/null || true)
    die "$target already exists (taken_utc=${taken:-unknown}); a new pre-upgrade backup never reuses it. \
Retire it first: mv -T $target $target.retired-$(date -u +%Y%m%dT%H%M%SZ)"
fi
unit_active || die "$SPARROW_UNIT is not running; the backup drains and restarts a running daemon"

# Stamps are fixed-width UTC (YYYYMMDDTHHMMSSZ), so they compare as strings.
started=$(date -u +%Y%m%dT%H%M%SZ)
note "backing up the store as $current left it (started $started)"
if [[ $SPARROW_SCOPE == system ]]; then
    # Type=oneshot: start returns once the backup has finished, and fails if it failed.
    systemctl start sparrow-backup.service
else
    runtime=$(mktemp -d)
    trap 'rm -rf -- "$runtime"' EXIT
    config_dir=$(dirname -- "$SPARROW_CONFIG")
    cli_dir=$(dirname -- "$CLI_LINK")
    RUNTIME_DIRECTORY=$runtime PATH=$cli_dir:$PATH python3 -I "$LIBEXEC/sparrow-backup" \
        "$SPARROW_CONFIG" "$SPARROW_BACKUPS" 3 "$SPARROW_INTAKE" "$config_dir"
fi

# This run's backup: the newest STAMP directory taken at or after the start. An older one, left by
# an earlier run, never qualifies.
shopt -s nullglob
fresh=
for dir in "$SPARROW_BACKUPS"/2*Z; do
    [[ -d $dir && ${dir##*/} =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || continue
    taken=$(sed -n 's/^taken_utc=//p' "$dir/SOURCE")
    if [[ -n $taken && ! $taken < $started ]]; then fresh=$dir; fi
done
[[ -n $fresh ]] || die "the backup left no backup taken at or after $started in $SPARROW_BACKUPS; \
read its log (journalctl -u sparrow-backup.service) before upgrading"
grep -qx "image=$current" "$fresh/SOURCE" \
    || die "$fresh does not record image=$current; the image changed during the backup"
now=$(readlink -- "$LIBEXEC/sparrowd")
[[ $now == "$current" ]] || die "the image link moved from $current to $now during the backup"

# Under the backup's lock, so the rotation cannot remove the directory while it is renamed. The
# rename takes it out of the three-backup rotation, which never prunes pre-upgrade-* names.
flock -w 60 "$SPARROW_BACKUPS/.lock" mv -T -n -- "$fresh" "$target"
[[ ! -e $fresh && -d $target ]] || die "$target appeared during the backup; $fresh is left in place"
sync -- "$SPARROW_BACKUPS"
source_lines=$(grep -E '^(taken_utc|image|store_id|schema_version|store_format)=' "$target/SOURCE")
mapfile -t source_lines <<<"$source_lines"
note "pre-upgrade backup $target:"
printf '  %s\n' "${source_lines[@]}"
