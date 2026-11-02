#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Install everything Sparrow's units need onto this host, idempotently, and print what changed:
# the user and group, the /SPARROW layout and its ACLs, /etc/sparrow (templates only where a file
# is absent; never a secret), the units, the AppArmor profile (loaded), the helper scripts, the
# kept images from ./bin, the lexicon bundle and these documents. Then daemon-reload and enable.
# It never starts, stops or restarts a unit: start.sh and restart.sh do that.
#
# Run as root. With SPARROW_DESTDIR=DIR it stages every file under DIR instead and skips each
# step that changes the running host (users, ownership, ACLs, AppArmor, systemd), printing it.
set -euo pipefail
umask 022

if (($# == 1)) && [[ $1 == -h || $1 == --help ]]; then
    cat <<'USAGE'
Usage: deploy/install.sh [--rev REV] [--no-enable]

Install units, the AppArmor profile, configuration templates, helpers and the image pair for REV
(default: the newest in ./bin). --no-enable leaves unit enablement as it is.
Set SPARROW_DESTDIR=DIR to stage the files under DIR without touching the host.
USAGE
    exit 0
fi

DEST=${SPARROW_DESTDIR:-}
PREFIX=${SPARROW_PREFIX:-/usr/local}
if [[ -n $DEST ]]; then
    [[ $DEST == /* ]] || { printf 'install.sh: SPARROW_DESTDIR must be absolute\n' >&2; exit 2; }
    [[ $PREFIX != "$DEST"* ]] || { printf 'install.sh: SPARROW_PREFIX is the installed prefix (default /usr/local); it must not include SPARROW_DESTDIR\n' >&2; exit 2; }
    export SPARROW_PREFIX=$DEST$PREFIX
fi
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

REV='' ENABLE=1
while (($#)); do
    case $1 in
        --rev) REV=${2:?--rev needs a revision}; shift 2 ;;
        --no-enable) ENABLE=0; shift ;;
        *) die "unknown argument $1 (see --help)" ;;
    esac
done
if [[ -z $DEST && $EUID -ne 0 ]]; then
    die "run as root (sudo deploy/install.sh), or set SPARROW_DESTDIR to stage into a directory"
fi

CHANGED=()
UNITS_CHANGED=0
PROFILE_CHANGED=0

# Run a host-changing command, or print it when staging.
host() {
    if [[ -n $DEST ]]; then
        note "staged, not run: $*"
    else
        "$@"
    fi
}

# Install SOURCE as TARGET with MODE and OWNER unless it is already identical.
put() {
    local source=$1 target=$DEST$2 mode=$3 owner=${4:-root:root}
    if [[ -f $target ]] && cmp -s -- "$source" "$target"; then
        return 1
    fi
    if [[ -n $DEST ]]; then
        install -D -m "$mode" -- "$source" "$target"
    else
        install -D -m "$mode" -o "${owner%:*}" -g "${owner#*:}" -- "$source" "$target"
    fi
    CHANGED+=("$2")
}

# Install a template as TARGET only when TARGET is absent: an operator's file is never replaced.
template() {
    local source=$1 target=$DEST$2
    if [[ -e $target ]]; then
        note "kept existing $2"
        return
    fi
    put "$source" "$2" 0640 root:sparrow || true
    note "created $2 from ${source#"$REPO_DIR"/}; edit it before the first start"
}

# 1. The sparrow user and group, and the operator's membership.
if put "$DEPLOY_DIR/config/sysusers-sparrow.conf" /etc/sysusers.d/sparrow.conf 0644; then :; fi
host systemd-sysusers sparrow.conf

# 2. /SPARROW, its modes and ACLs (re-asserted at every boot by systemd-tmpfiles).
if put "$DEPLOY_DIR/config/tmpfiles-sparrow.conf" /etc/tmpfiles.d/sparrow.conf 0644; then :; fi
host systemd-tmpfiles --create sparrow.conf

# 3. /etc/sparrow: configuration templates, watch definitions, and the secret's location.
if [[ -n $DEST ]]; then
    install -d -m 0750 -- "$DEST/etc/sparrow" "$DEST/etc/sparrow/watches"
else
    install -d -m 0750 -o root -g sparrow -- /etc/sparrow /etc/sparrow/watches
fi
template "$DEPLOY_DIR/config/server.toml.example" /etc/sparrow/server.toml
template "$DEPLOY_DIR/config/ship.toml.example" /etc/sparrow/ship.toml
if grep -q '^rpc_tcp_address' "$DEST/etc/sparrow/server.toml"; then
    secret=$DEST/etc/sparrow/tcp.secret
    if [[ ! -f $secret ]]; then
        note "MISSING $secret: the public listener needs it (README.md, step 4); the unit will not start without it"
    elif [[ -z $DEST && $(stat -c '%U:%a' -- "$secret") != root:600 ]]; then
        die "$secret must be root-owned mode 0600, not $(stat -c '%U:%a' -- "$secret")"
    fi
fi

# 4. Units.
for unit in "$DEPLOY_DIR"/systemd/*.service "$DEPLOY_DIR"/systemd/*.timer; do
    if put "$unit" "/etc/systemd/system/${unit##*/}" 0644; then UNITS_CHANGED=1; fi
done

# 5. The AppArmor profile, in enforce mode, loaded now and at boot by apparmor.service.
if put "$DEPLOY_DIR/apparmor/sparrow" /etc/apparmor.d/sparrow 0644; then PROFILE_CHANGED=1; fi
if [[ -z $DEST ]] && ! grep -qx 'sparrow (enforce)' /sys/kernel/security/apparmor/profiles; then
    PROFILE_CHANGED=1
fi
if ((PROFILE_CHANGED)); then
    host apparmor_parser -r -W /etc/apparmor.d/sparrow
    [[ -n $DEST ]] || CHANGED+=("AppArmor profile sparrow loaded")
fi

# 6. Helpers run by the timers.
for helper in sparrow-ship sparrow-backup; do
    if put "$DEPLOY_DIR/libexec/$helper" "$PREFIX/libexec/sparrow/$helper" 0755; then :; fi
done

# 7. The image pair, under its own names; the stable links only when none exist yet.
read -r daemon cli < <(select_images "$REV")
for image in "$daemon" "$cli"; do
    [[ -e $LIBEXEC/${image##*/} ]] || CHANGED+=("$PREFIX/libexec/sparrow/${image##*/}")
done
daemon=$(install_image "$daemon")
cli=$(install_image "$cli")
verify_image "$daemon" 'usage: sparrowd --config'
verify_image "$cli" 'usage: sparrow'
if [[ -L $LIBEXEC/sparrowd ]]; then
    note "kept image links (sparrowd -> $(readlink -- "$LIBEXEC/sparrowd")); start.sh or restart.sh switches images"
else
    select_current "$daemon" "$cli"
    CHANGED+=("image links -> ${daemon##*/}")
fi

# 8. The lexicon bundle for the PurRDF revision the images were built against.
for bundle in "$SPARROW_BIN"/lexicons-*; do
    [[ -d $bundle ]] || die "no lexicon bundle in $SPARROW_BIN; run make install-bin"
    target=$DEST$PREFIX/share/sparrow/${bundle##*/}
    if [[ -d $target ]]; then
        diff -r -q -- "$bundle" "$target" >/dev/null || die "$target differs from $bundle; refusing to replace it"
        continue
    fi
    install -d -m 0755 -- "$(dirname -- "$target")"
    staging=$(mktemp -d "$(dirname -- "$target")/.lexicons.XXXXXX")
    cp -a --reflink=auto -- "$bundle/." "$staging/"
    diff -r -q -- "$bundle" "$staging" >/dev/null || die "copy of $bundle does not match"
    chmod -R a+rX,go-w -- "$staging"
    mv -T -n -- "$staging" "$target"
    CHANGED+=("$PREFIX/share/sparrow/${bundle##*/}")
done

# 9. These documents, for the operator of a host without the repository.
for document in "$DEPLOY_DIR"/*.md "$DEPLOY_DIR"/evidence/*.md; do
    if put "$document" "$PREFIX/share/doc/sparrow/deploy/${document#"$DEPLOY_DIR"/}" 0644; then :; fi
done

# 10. systemd.
if ((UNITS_CHANGED)); then
    host systemctl daemon-reload
fi
if ((ENABLE)); then
    if [[ -n $DEST ]]; then
        note "skipped while staging: the lillith.slice check and the old user unit check (README.md, step 1)"
    else
        [[ -n $(systemctl show -p FragmentPath --value lillith.slice) ]] \
            || die "lillith.slice is not installed; install Katamari's deploy/systemd/lillith.slice first (README.md, step 1)"
        for wants in /home/*/.config/systemd/user/default.target.wants/sparrow.service; do
            [[ ! -e $wants ]] || die "the old user unit is still enabled ($wants); disable it first (README.md, Moving a running service)"
        done
    fi
    host systemctl enable sparrow.service sparrow-ship.timer sparrow-backup.timer
fi

if ((${#CHANGED[@]})); then
    printf 'install.sh: changed:\n'
    printf '  %s\n' "${CHANGED[@]}"
else
    printf 'install.sh: nothing changed\n'
fi
