#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Replace the running image with a kept one, or roll back to the previous image. The candidate is
# installed and verified while the old daemon still runs; the stop goes through the drain; the
# image the links named becomes the rollback (.previous). After the start the store identity must
# be the one the old daemon reported. There is no automatic rollback: an image may upgrade the
# store's format, and an older image cannot read it (RECOVERY.md, "Rollback").
set -euo pipefail
umask 022

if (($# == 1)) && [[ $1 == -h || $1 == --help ]]; then
    cat <<'USAGE'
Usage: deploy/restart.sh [--rev REV]
       deploy/restart.sh --rollback

Without --rollback, restart sparrow.service on the image pair for REV (default: the newest pair in
./bin), keeping the current pair as the rollback. With --rollback, swap back to the previous pair.
USAGE
    exit 0
fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

MODE=forward REV=''
case ${1:-} in
    --rollback) MODE=rollback; shift ;;
    --rev) REV=${2:?--rev needs a revision}; shift 2 ;;
    '') ;;
    *) die "unknown argument $1 (see --help)" ;;
esac
(($# == 0)) || die "unexpected arguments: $*"
WORK=$(mktemp -d)
trap 'rm -rf -- "$WORK"' EXIT

if [[ $MODE == rollback ]]; then
    previous=$(readlink -- "$LIBEXEC/sparrowd.previous" 2>/dev/null) || die "no previous image to roll back to"
    daemon=$LIBEXEC/$previous
    cli=$LIBEXEC/$(readlink -- "$LIBEXEC/sparrow.previous")
    note "rolling back to $previous; an image cannot undo a store-format upgrade its successor made"
else
    read -r daemon cli < <(select_images "$REV")
    daemon=$(install_image "$daemon")
    cli=$(install_image "$cli")
fi
verify_image "$daemon" 'usage: sparrowd --config'
verify_image "$cli" 'usage: sparrow'

store=
if unit_active; then
    store=$(status_json | status_field store_id)
    [[ $(readlink -- "$LIBEXEC/sparrowd") != "${daemon##*/}" ]] || note "already on ${daemon##*/}; restarting it"
fi
drain_stop
select_current "$daemon" "$cli"
note "starting $SPARROW_UNIT on ${daemon##*/}"
"${SYSTEMCTL[@]}" start "$SPARROW_UNIT"
wait_ready
if [[ -n $store ]]; then
    now=$(status_json | status_field store_id)
    [[ $now == "$store" ]] || die "store identity changed from $store to $now; stop and read RECOVERY.md"
fi
receipt
