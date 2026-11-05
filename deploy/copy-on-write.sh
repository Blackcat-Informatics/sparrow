#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Switch copy-on-write off for the store, by the functions in lib.sh. For steps that run outside
# the other deploy scripts: a restore (RECOVERY.md) and a store conversion at a restart.
set -euo pipefail
umask 022

usage() {
    cat <<'USAGE'
Usage: deploy/copy-on-write.sh off DIR
       deploy/copy-on-write.sh match SOURCE DIR
       deploy/copy-on-write.sh convert DATA_DIR

off      switch copy-on-write off for DIR, so what is created in it inherits that
         (left as it is if DIR already holds copy-on-write files)
match    give the empty DIR the copy-on-write attribute of SOURCE, before a reflink copy
convert  rewrite the closed store DATA_DIR with copy-on-write off, keeping the original as
         DATA_DIR.cow-STAMP
USAGE
}
if (($# == 1)) && [[ $1 == -h || $1 == --help ]]; then usage; exit 0; fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
case ${1:-}:$# in
    off:2) switch_off_copy_on_write "$2" ;;
    match:3) match_copy_on_write "$2" "$3" ;;
    convert:2) convert_store_no_cow "$2" ;;
    *) usage >&2; exit 2 ;;
esac
