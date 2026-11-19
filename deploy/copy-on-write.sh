#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Switch copy-on-write off for the store, by the functions in lib.sh. For steps that run outside
# the other deploy scripts: a restore (RECOVERY.md), a store conversion at a restart and the copy
# kept before a one-way step.
set -euo pipefail
umask 022

usage() {
    cat <<'USAGE'
Usage: deploy/copy-on-write.sh off DIR
       deploy/copy-on-write.sh match SOURCE DIR
       deploy/copy-on-write.sh convert DATA_DIR
       deploy/copy-on-write.sh copy SOURCE DEST

off      switch copy-on-write off for DIR, so what is created in it inherits that
         (left as it is if DIR already holds copy-on-write files)
match    give the empty DIR the copy-on-write attribute of SOURCE, before a reflink copy
convert  rewrite the closed store DATA_DIR with copy-on-write off, keeping the original as
         DATA_DIR.cow-STAMP
copy     copy the closed store SOURCE to the new directory DEST (a reflink copy when SOURCE and
         every file in it have copy-on-write off, a full copy otherwise), compare the file
         lists and give DEST its name only then; refuses unless the filesystem holds SOURCE's
         size plus 10 GiB
USAGE
}
if (($# == 1)) && [[ $1 == -h || $1 == --help ]]; then usage; exit 0; fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
case ${1:-}:$# in
    off:2) switch_off_copy_on_write "$2" ;;
    match:3) match_copy_on_write "$2" "$3" ;;
    convert:2) convert_store_no_cow "$2" ;;
    copy:3) keep_store_copy "$2" "$3" ;;
    *) usage >&2; exit 2 ;;
esac
