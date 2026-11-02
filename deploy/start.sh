#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Start Sparrow from a kept image in ./bin: install the image pair under its own names, verify it,
# point the stable links at it, start the unit and wait until the daemon answers on its socket.
# Run as root for the production unit. deploy/README.md describes the whole procedure.
set -euo pipefail
umask 022

if (($# == 1)) && [[ $1 == -h || $1 == --help ]]; then
    cat <<'USAGE'
Usage: deploy/start.sh [--rev REV]

Start sparrow.service from the image pair bin/sparrowd-schemaN-REV and bin/sparrow-schemaN-REV
(default: the newest pair in ./bin). Refuses if the unit already runs another image; use
restart.sh to replace a running image.
USAGE
    exit 0
fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

REV=
case ${1:-} in
    --rev) REV=${2:?--rev needs a revision}; shift 2 ;;
    '') ;;
    *) die "unknown argument $1 (see --help)" ;;
esac
(($# == 0)) || die "unexpected arguments: $*"
WORK=$(mktemp -d)
trap 'rm -rf -- "$WORK"' EXIT

read -r daemon cli < <(select_images "$REV")
if unit_active; then
    [[ $(readlink -- "$LIBEXEC/sparrowd") == "${daemon##*/}" ]] \
        || die "$SPARROW_UNIT already runs $(readlink -- "$LIBEXEC/sparrowd"); use restart.sh"
    note "$SPARROW_UNIT already runs ${daemon##*/}"
    receipt
    exit 0
fi

check_config() {
    local socket data lexicons
    socket=$(config_value socket_path)
    data=$(config_value data_dir)
    [[ $(dirname -- "$socket") == "$data" ]] || die "socket_path $socket must be directly inside data_dir $data"
    lexicons=$(config_value text_lexicon_dir)
    [[ $(find "$lexicons" -maxdepth 1 -name '*.cbor' | wc -l) == 5 ]] \
        || die "text_lexicon_dir $lexicons must hold the five native CBOR lexicons"
}
check_config
daemon=$(install_image "$daemon")
cli=$(install_image "$cli")
verify_image "$daemon" 'usage: sparrowd --config'
verify_image "$cli" 'usage: sparrow'
select_current "$daemon" "$cli"
note "starting $SPARROW_UNIT on ${daemon##*/}"
"${SYSTEMCTL[@]}" start "$SPARROW_UNIT"
wait_ready
receipt
