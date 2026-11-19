#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# A disposable test instance for rehearsing start.sh, stop.sh, restart.sh and check.sh without
# root and without touching the production unit: install.sh stages the files under
# SCRATCH/root, the configuration gets a scratch store and socket and no TCP listener, and a user
# unit sparrow-deploytest.service is rendered from deploy/systemd/sparrow.service into the user
# manager's runtime directory ($XDG_RUNTIME_DIR/systemd/user, gone at logout).
#
# The rendered unit keeps the production sandbox (seccomp filter, MemoryDenyWriteExecute,
# ProtectSystem=strict, address families, namespaces, resource limits) and drops only what an
# unprivileged user manager cannot apply: the user and group, the credential, the AppArmor
# profile, the capability, the slice, negative nice and OOM adjustment. ProtectHome= becomes
# read-only because the scratch directory is under /home. deploy/evidence/lifecycle.md records a run.
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/test-instance.sh setup [SCRATCH]
       deploy/test-instance.sh env [SCRATCH]
       deploy/test-instance.sh teardown [SCRATCH]

setup stages a test instance in SCRATCH (default: .stage/test-instance in the repository) and
prints the environment that points the lifecycle scripts at it; env prints it again; teardown
stops the test unit and removes it (the scratch directory is left for inspection).
USAGE
}
(($# >= 1)) || { usage >&2; exit 2; }
ACTION=$1
# For DEPLOY_DIR, REPO_DIR, SPARROW_BIN, die and require_socket_length.
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
SCRATCH=${2:-$REPO_DIR/.stage/test-instance}
UNIT=sparrow-deploytest.service
UNIT_DIR=${XDG_RUNTIME_DIR:?XDG_RUNTIME_DIR is required}/systemd/user
ROOT=$SCRATCH/root
DATA=$SCRATCH/data

environment() {
    printf 'export SPARROW_SCOPE=user SPARROW_UNIT=%s\n' "$UNIT"
    printf 'export SPARROW_PREFIX=%q SPARROW_CONFIG=%q\n' "$ROOT/usr/local" "$ROOT/etc/sparrow/server.toml"
    printf 'export SPARROW_BIN=%q\n' "${SPARROW_BIN:-$REPO_DIR/bin}"
}

case $ACTION in
    setup)
        [[ $SCRATCH == /* ]] || { echo "test-instance.sh: SCRATCH must be absolute" >&2; exit 2; }
        require_socket_length "$DATA/store/rpc.sock"
        install -d -m 0700 -- "$SCRATCH" "$DATA" "$DATA/intake" "$DATA/intake/deploytest"
        bundle=$(find "$SPARROW_BIN" -mindepth 1 -maxdepth 1 -type d -name 'lexicons-*' | head -n 1)
        [[ -n $bundle ]] || die "no lexicon bundle in $SPARROW_BIN; run make install-bin"
        lexicons=$ROOT/usr/local/share/sparrow/${bundle##*/}
        # Written first, so install.sh keeps it instead of the production template and its secret.
        install -d -m 0750 -- "$ROOT/etc/sparrow"
        cat >"$ROOT/etc/sparrow/server.toml" <<CONFIG
# Test instance (deploy/test-instance.sh): scratch store and socket, no TCP listener.
socket_path = "$DATA/store/rpc.sock"
data_dir = "$DATA/store"
instance_role = "scratch"
disk_budget = 1073741824
maintenance_reserve = 67108864
max_records = 100000
max_pin_members = 10000
max_concurrent_requests = 2
max_rpc_connections = 8
buffered_flush_interval_ms = 2000
text_lexicon_dir = "$lexicons/artifacts"

[[consume]]
id = "deploytest"
root = "$DATA/intake/deploytest"
producer_contract = "finished_files"
source = "deploytest"
format = "jsonl"
quiet_ms = 1000
max_directories = 1
max_candidates = 64
CONFIG
        SPARROW_DESTDIR=$ROOT "$DEPLOY_DIR/install.sh" --no-enable
        install -d -m 0700 -- "$UNIT_DIR"
        sed -E \
            -e '/^(User|Group|LoadCredential|AppArmorProfile|AmbientCapabilities|CapabilityBoundingSet|Slice|Nice|OOMScoreAdjust|ManagedOOMPreference|RequiresMountsFor)=/d' \
            -e "s|^ExecStart=.*|ExecStart=$ROOT/usr/local/libexec/sparrow/sparrowd --config $ROOT/etc/sparrow/server.toml|" \
            -e "s|^WorkingDirectory=.*|WorkingDirectory=$DATA|" \
            -e "s|^ReadWritePaths=.*|ReadWritePaths=$DATA|" \
            -e 's|^ProtectHome=.*|ProtectHome=read-only|' \
            -e 's|^Description=.*|Description=Sparrow deploy test instance (deploy/test-instance.sh)|' \
            -e '/^\[Install\]/,$d' \
            "$DEPLOY_DIR/systemd/sparrow.service" >"$UNIT_DIR/$UNIT"
        systemctl --user daemon-reload
        echo "test-instance.sh: $UNIT rendered into $UNIT_DIR; drive it with:"
        environment
        ;;
    env)
        environment
        ;;
    teardown)
        systemctl --user stop "$UNIT" 2>/dev/null || true
        rm -f -- "$UNIT_DIR/$UNIT"
        systemctl --user daemon-reload
        echo "test-instance.sh: $UNIT removed; $SCRATCH kept"
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
