#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Stop Sparrow through systemd and the daemon's own SIGINT drain (workers drained, WAL
# synchronized, socket removed), bounded by the unit's TimeoutStopSec. Reports the claims it still
# held and whether the drain finished. The timers are left as they are: the backup refuses to run
# while the service is stopped, and the shipper only fills roots.
set -euo pipefail
umask 022

if (($# == 1)) && [[ $1 == -h || $1 == --help ]]; then
    printf 'Usage: deploy/stop.sh\n\nStop sparrow.service through its drain and report what it held.\n'
    exit 0
fi
(($# == 0)) || { printf 'stop.sh: takes no arguments (see --help)\n' >&2; exit 2; }
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
WORK=$(mktemp -d)
trap 'rm -rf -- "$WORK"' EXIT

drain_stop
if [[ $SPARROW_SCOPE == system ]]; then
    for timer in sparrow-ship.timer sparrow-backup.timer; do
        note "$timer is $("${SYSTEMCTL[@]}" is-active "$timer" || true)"
    done
fi
