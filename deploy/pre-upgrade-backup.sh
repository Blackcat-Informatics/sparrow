#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Take the named pre-upgrade backup that start.sh and restart.sh require before an image that
# raises the schema (README.md, "Upgrades and rollback"). It runs one online backup now (the
# daemon keeps serving; nothing is stopped) and points SPARROW_BACKUPS/pre-upgrade-CURRENT, where
# CURRENT is the image the links name, at that run's set sets/ID. It stops at the first failure,
# refuses an existing pre-upgrade-CURRENT, and accepts only a set whose taken_utc is from this run
# and whose image= is CURRENT. The engine keeps backup_keep backups, so the link is good for the
# upgrade it was taken for: start.sh and restart.sh accept it for PRE_UPGRADE_MAX_HOURS.
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
unit_active || die "$SPARROW_UNIT is not running; the running daemon takes the online backup"

started=$(date -u +%s)
note "backing up the store as $current left it (started $started)"
if [[ $SPARROW_SCOPE == system ]]; then
    # Type=oneshot: start returns once the backup has finished, and fails if it failed.
    systemctl start sparrow-backup.service
else
    config_dir=$(dirname -- "$SPARROW_CONFIG")
    cli_dir=$(dirname -- "$CLI_LINK")
    PATH=$cli_dir:$PATH python3 -I "$LIBEXEC/sparrow-backup" \
        "$SPARROW_CONFIG" "$SPARROW_INTAKE" "$config_dir"
fi

# This run's set: the newest sets/ID taken at or after the start. An older one, left by an
# earlier run, never qualifies.
shopt -s nullglob
fresh=
for dir in "$SPARROW_BACKUPS"/sets/*; do
    [[ -d $dir && ${dir##*/} =~ ^[0-9]+$ ]] || continue
    taken=$(sed -n 's/^taken_utc=//p' "$dir/SOURCE")
    taken_at=$(date -u -d "$taken" +%s 2>/dev/null) || continue
    # The daemon's clock stamps the start to the second, after this script read its own.
    if ((taken_at + 1 >= started)); then fresh=$dir; fi
done
[[ -n $fresh ]] || die "the backup left no set taken at or after $(date -u -d "@$started" +%FT%TZ) in $SPARROW_BACKUPS; \
read its log (journalctl -u sparrow-backup.service) before upgrading"
grep -qx "image=$current" "$fresh/SOURCE" \
    || die "$fresh does not record image=$current; the image changed during the backup"
now=$(readlink -- "$LIBEXEC/sparrowd")
[[ $now == "$current" ]] || die "the image link moved from $current to $now during the backup"

# A relative link, so the backup root can move or be mounted elsewhere for a restore.
ln -s -- "sets/${fresh##*/}" "$target" || die "$target appeared during the backup"
[[ -d $target ]] || die "$target does not lead to $fresh"
sync -- "$SPARROW_BACKUPS"
source_lines=$(grep -E '^(taken_utc|image|store_id|schema_version|store_format)=' "$target/SOURCE")
mapfile -t source_lines <<<"$source_lines"
note "pre-upgrade backup $target:"
printf '  %s\n' "${source_lines[@]}"
