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
       deploy/start.sh --restored-from BACKUP

Start sparrow.service from the image pair bin/sparrowd-schemaN-REV and bin/sparrow-schemaN-REV
(default: the newest pair in ./bin). Refuses if the unit already runs another image; use
restart.sh to replace a running image.

--restored-from starts a store restored from BACKUP (RECOVERY.md, "Restore from a backup") on
exactly the image that BACKUP/SOURCE records as having last opened it, and checks that the daemon
reports the store identity recorded there. It is the only way to start an older schema.
USAGE
    exit 0
fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

REV='' RESTORED='' RESTORED_STORE=''
case ${1:-} in
    --rev) REV=${2:?--rev needs a revision}; shift 2 ;;
    --restored-from) RESTORED=${2:?--restored-from needs a backup directory}; shift 2 ;;
    '') ;;
    *) die "unknown argument $1 (see --help)" ;;
esac
(($# == 0)) || die "unexpected arguments: $*"
WORK=$(mktemp -d)
trap 'rm -rf -- "$WORK"' EXIT
hold_backup_lock

if [[ -n $RESTORED ]]; then
    [[ -r $RESTORED/SOURCE ]] || die "cannot read $RESTORED/SOURCE"
    image=$(sed -n 's/^image=//p' "$RESTORED/SOURCE")
    RESTORED_STORE=$(sed -n 's/^store_id=//p' "$RESTORED/SOURCE")
    [[ $image == sparrowd-schema*-* && -n $RESTORED_STORE ]] \
        || die "$RESTORED/SOURCE records no image and store_id; it predates image recording, so choose the image by hand with care"
    if [[ -f $LIBEXEC/$image ]]; then
        daemon=$LIBEXEC/$image cli=$LIBEXEC/sparrow-${image#sparrowd-}
    elif [[ -f $SPARROW_BIN/$image ]]; then
        daemon=$SPARROW_BIN/$image cli=$SPARROW_BIN/sparrow-${image#sparrowd-}
    else
        # The backup keeps the pair, so a restore on a fresh host needs nothing from ./bin.
        daemon=$RESTORED/images/$image cli=$RESTORED/images/sparrow-${image#sparrowd-}
        [[ -f $daemon && -f $cli ]] || die "no image pair for $image in $LIBEXEC, $SPARROW_BIN or $RESTORED/images"
    fi
    ! unit_active || die "$SPARROW_UNIT is running; stop it before starting a restored store"
    note "restored store: starting $image, the image that last opened it ($RESTORED)"
else
    images=$(select_images "$REV") || exit 1
    read -r daemon cli <<<"$images"
fi
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
    require_socket_length "$socket"
    lexicons=$(config_value text_lexicon_dir)
    [[ $(find "$lexicons" -maxdepth 1 -name '*.cbor' | wc -l) == 5 ]] \
        || die "text_lexicon_dir $lexicons must hold the five native CBOR lexicons"
}
check_config
# The checks run on the chosen images before anything is installed, so a refusal changes nothing.
verify_image "$daemon" 'usage: sparrowd --config'
verify_image "$cli" 'usage: sparrow'
if [[ -z $RESTORED ]]; then
    refuse_schema_downgrade "$daemon"
    require_pre_upgrade_backup "$daemon"
fi
refuse_start_limit
daemon=$(install_image "$daemon")
cli=$(install_image "$cli")
select_current "$daemon" "$cli"
note "starting $SPARROW_UNIT on ${daemon##*/}"
"${SYSTEMCTL[@]}" start "$SPARROW_UNIT"
wait_ready
if [[ -n $RESTORED ]]; then
    now=$(status_json | status_field store_id)
    [[ $now == "$RESTORED_STORE" ]] \
        || die "the daemon reports store $now, but $RESTORED recorded $RESTORED_STORE; stop it and check the restore"
fi
receipt
