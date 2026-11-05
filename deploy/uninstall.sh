#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Undo what deploy/install.sh changed on the host, so the operator's old user unit can serve
# /SPARROW again (the revert of Sparrow issue #22): disable the system units and remove them,
# unload and remove the AppArmor profile, remove the tmpfiles entry that re-owns /SPARROW at every
# boot and the CLI link, run daemon-reload, and give /SPARROW, /SPARROW/store and /SPARROW/intake
# back to the operator, mode 0700 and without ACLs, as they were before the cutover.
#
# It leaves in place, and says so: /etc/sparrow (configuration and the secret's copy), the sparrow
# user and group and their sysusers entry, the installed images, helpers, lexicons and documents,
# /SPARROW/ship and /SPARROW/backups. It never starts or stops a unit: stop them first.
#
# Run as root. With SPARROW_DESTDIR=DIR it removes the staged files under DIR instead and prints
# each step that would change the running host (systemd, AppArmor, ownership, ACLs).
set -euo pipefail
umask 022

if (($# == 1)) && [[ $1 == -h || $1 == --help ]]; then
    cat <<'USAGE'
Usage: deploy/uninstall.sh

Remove the system units, the AppArmor profile, the tmpfiles entry and the CLI link that
install.sh installed, and return /SPARROW to the operator. Stop the units first.
Set SPARROW_DESTDIR=DIR to remove the files staged under DIR without touching the host.
USAGE
    exit 0
fi
(($# == 0)) || { printf 'uninstall.sh: takes no arguments (see --help)\n' >&2; exit 2; }

DEST=${SPARROW_DESTDIR:-}
PREFIX=${SPARROW_PREFIX:-/usr/local}
if [[ -n $DEST ]]; then
    [[ $DEST == /* ]] || { printf 'uninstall.sh: SPARROW_DESTDIR must be absolute\n' >&2; exit 2; }
    [[ $PREFIX != "$DEST"* ]] || { printf 'uninstall.sh: SPARROW_PREFIX is the installed prefix (default /usr/local); it must not include SPARROW_DESTDIR\n' >&2; exit 2; }
    export SPARROW_PREFIX=$DEST$PREFIX
fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
if [[ -z $DEST && $EUID -ne 0 ]]; then
    die "run as root (sudo deploy/uninstall.sh), or set SPARROW_DESTDIR to remove a staged install"
fi

# Preconditions, all read-only, before anything changes.
problems=()
accounts=$(operators) || exit 1
read -r -a operator_list <<<"$accounts"
((${#operator_list[@]} == 1)) \
    || problems+=("config/sysusers-sparrow.conf names ${#operator_list[@]} operators; /SPARROW goes back to exactly one")
operator=${operator_list[0]:-}
if [[ -n $operator ]] && ! getent passwd "$operator" >/dev/null; then
    problems+=("no account $operator to give /SPARROW back to")
fi
units=()
for unit in "$DEPLOY_DIR"/systemd/*.service "$DEPLOY_DIR"/systemd/*.timer; do
    units+=("${unit##*/}")
done
profile_loaded=0
if [[ -n $DEST ]]; then
    note "staging: the units' states and the loaded profile are not checked"
else
    for unit in "${units[@]}"; do
        state=$(systemctl is-active "$unit" || true)
        case $state in
            active | activating | deactivating | reloading | refreshing)
                problems+=("$unit is $state; stop it first (deploy/stop.sh for sparrow.service, systemctl stop for the timers and their services)") ;;
        esac
    done
    profiles=$(cat /sys/kernel/security/apparmor/profiles) || exit 1
    if grep -q '^sparrow (' <<<"$profiles"; then profile_loaded=1; fi
    if ((profile_loaded)) && [[ ! -f /etc/apparmor.d/sparrow ]]; then
        problems+=("the sparrow AppArmor profile is loaded but /etc/apparmor.d/sparrow is gone; unload it by hand")
    fi
fi
if ((${#problems[@]})); then
    printf 'uninstall.sh: refused; nothing was changed:\n' >&2
    printf '  %s\n' "${problems[@]}" >&2
    exit 1
fi
trap report_partial EXIT

# Remove a file install.sh installed, if present.
remove() {
    [[ -e $DEST$1 || -L $DEST$1 ]] || return 0
    rm -f -- "$DEST$1"
    CHANGED+=("removed $1")
}

# 1. Disable the units that are installed, then remove every unit file.
enabled=()
for unit in sparrow.service sparrow-ship.timer sparrow-backup.timer; do
    if [[ -e $DEST/etc/systemd/system/$unit ]]; then enabled+=("$unit"); fi
done
((${#enabled[@]} == 0)) || host systemctl disable "${enabled[@]}"
units_removed=0
for unit in "${units[@]}"; do
    if [[ -e $DEST/etc/systemd/system/$unit ]]; then units_removed=1; fi
    remove "/etc/systemd/system/$unit"
done

# 2. The AppArmor profile: unloaded from the kernel, then removed (apparmor.service would load it
# again at boot).
if ((profile_loaded)); then
    host apparmor_parser -R /etc/apparmor.d/sparrow
fi
remove /etc/apparmor.d/sparrow

# 3. The tmpfiles entry, which would re-own /SPARROW to sparrow at the next boot.
remove /etc/tmpfiles.d/sparrow.conf

# 4. The CLI link, which would shadow the operator's own CLI on PATH. The images it names stay.
if [[ -L $CLI_LINK && $(readlink -- "$CLI_LINK") == "$LIBEXEC/sparrow" ]]; then
    remove "$PREFIX/bin/sparrow"
fi

# 5. systemd forgets the removed units.
if ((units_removed)); then
    host systemctl daemon-reload
fi

# 6. /SPARROW back to the operator, as before the cutover: owner OPERATOR, group root, mode 0700, no
# ACL; the store and the intake tree the operator's, directories 0700, without ACLs.
if [[ -n $DEST || -d /SPARROW ]]; then
    host chown "$operator:root" /SPARROW
    host chmod 0700 /SPARROW
    host setfacl -b /SPARROW
    for tree in /SPARROW/store /SPARROW/intake; do
        if [[ -n $DEST || -d $tree ]]; then
            host chown -R "$operator:$operator" "$tree"
            host setfacl -R -b "$tree"
            host find "$tree" -type d -exec chmod 0700 '{}' +
        fi
    done
else
    note "no /SPARROW; nothing to give back"
fi

report_changes
note "kept: /etc/sparrow, the sparrow user and group (/etc/sysusers.d/sparrow.conf), the images, \
helpers, lexicons and documents under $PREFIX, /SPARROW/ship and /SPARROW/backups"
