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
#
# The instance serves test loads too (deploy/README.md, "Test instance"): a second socket for
# senders, readable by one group (SPARROW_TEST_INGEST_GROUP, default the invoking user's primary
# group), and a disk budget (SPARROW_TEST_DISK_BUDGET) and free-space floor
# (SPARROW_TEST_MIN_FREE) sized for a load. It is a separate store with its own store_id and
# instance_role = "scratch", never the live store. SPARROW_TEST_ARCHIVE_DIR turns the archive worker
# on, writing to a directory that must be on its own mount (a tmpfs such as /dev/shm).
set -euo pipefail
umask 077

usage() {
    cat <<'USAGE'
Usage: deploy/test-instance.sh setup [SCRATCH]
       deploy/test-instance.sh env [SCRATCH]
       deploy/test-instance.sh teardown [SCRATCH]
       deploy/test-instance.sh purge [SCRATCH]

setup stages a test instance in SCRATCH (default: .stage/test-instance in the repository) and
prints the environment that points the lifecycle scripts at it, with SPARROW_TEST_INGEST_SOCKET
naming the ingest socket a sender uses; env prints it again; teardown
stops the test unit and removes it (the scratch directory, with its store, is kept); purge
removes the scratch store, intake, ingest socket directory and staged files, and only when the
unit is stopped and SCRATCH holds a configuration written by this script. SPARROW_TEST_INGEST_GROUP,
SPARROW_TEST_DISK_BUDGET (bytes, default 16 GiB) and SPARROW_TEST_MIN_FREE (bytes, default
4 GiB) shape setup. SPARROW_TEST_ARCHIVE_DIR, an existing directory the invoking user can write
and on a different device than SCRATCH (a tmpfs such as /dev/shm/NAME), adds the [archive] table
and its ReadWritePaths entry; unset, the instance has no archive. setup records the directory in
SCRATCH/archive-dir, and purge then removes that directory's STORE_ID file and <store_id>/
subtree, never the directory itself or anything else in it.
The directory must also be empty and satisfy the daemon's own-mount rule. purge removes the
subtree only when STORE_ID names this instance's store (teardown saves the id in
SCRATCH/store-id while the unit runs), and otherwise removes nothing.
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
INGEST=$SCRATCH/ingest
# The archive directory setup was given, recorded so that purge removes only what the daemon wrote there.
ARCHIVE_RECORD=$SCRATCH/archive-dir
# The store id of this instance's store, saved by teardown while the unit still answers, so purge can
# tell this instance's archive from another store's.
STORE_RECORD=$SCRATCH/store-id
CONFIG=$ROOT/etc/sparrow/server.toml

# The store id of this instance: the saved record, else the unit's journal line for this socket.
own_store_id() {
    local id=
    if [[ -f $STORE_RECORD ]]; then
        id=$(<"$STORE_RECORD")
    else
        id=$(journalctl --user -u "$UNIT" -o cat --no-pager 2>/dev/null \
            | grep -F "capnp=$DATA/store/rpc.sock data=" | tail -n 1 \
            | sed -nE 's/.*store=(store:[0-9a-f]{32}) .*/\1/p' || true)
    fi
    [[ $id =~ ^store:[0-9a-f]{32}$ ]] && printf '%s\n' "$id"
    return 0
}

# The daemon's own-mount rule (crates/sparrow-server/src/archive.rs, require_own_mount): a device other
# than the root's, and a filesystem type that is a network filesystem or differs from the root's.
require_own_mount() {
    local dir=$1 type root_type
    [[ $(stat -c %d -- "$dir") != "$(stat -c %d -- /)" ]] \
        || die "the archive directory $dir is on the same device as /, which the daemon refuses; use a directory on its own mount, such as one under /dev/shm"
    type=$(stat -f -c %t -- "$dir")
    root_type=$(stat -f -c %t -- /)
    case $type in
        6969 | 517b | ff534d42 | fe534d42 | c36400 | 6b414653 | 1021997 | bd00bd0 | 564c | 73757245) return 0 ;;
    esac
    [[ $type != "$root_type" ]] \
        || die "the archive directory $dir has filesystem type 0x$type, the same local type as / and not a network filesystem, which the daemon refuses; use a directory on its own mount, such as one under /dev/shm"
}

environment() {
    printf 'export SPARROW_SCOPE=user SPARROW_UNIT=%s\n' "$UNIT"
    printf 'export SPARROW_PREFIX=%q SPARROW_CONFIG=%q\n' "$ROOT/usr/local" "$ROOT/etc/sparrow/server.toml"
    printf 'export SPARROW_BIN=%q\n' "${SPARROW_BIN:-$REPO_DIR/bin}"
    printf 'export SPARROW_TEST_INGEST_SOCKET=%q\n' "$INGEST/ingest.sock"
}

case $ACTION in
    setup)
        [[ $SCRATCH == /* ]] || { echo "test-instance.sh: SCRATCH must be absolute" >&2; exit 2; }
        require_socket_length "$DATA/store/rpc.sock"
        require_socket_length "$INGEST/ingest.sock"
        # A store made before the role row existed has none, and the daemon refuses to start on it
        # once the configuration names a role. Its old configuration, still on disk, lacks the key.
        if [[ -e $DATA/store/CURRENT && -f $CONFIG ]] && ! grep -q '^instance_role = ' "$CONFIG"; then
            die "the store in $DATA/store was created before instance roles and has no role row, so the daemon refuses to start on it; run 'deploy/test-instance.sh purge $SCRATCH' to remove it and set up a fresh one"
        fi
        group=${SPARROW_TEST_INGEST_GROUP:-$(id -gn)}
        gid=$(getent group "$group" | cut -d: -f3)
        [[ -n $gid ]] || die "no group named $group for SPARROW_TEST_INGEST_GROUP"
        disk_budget=${SPARROW_TEST_DISK_BUDGET:-17179869184}
        min_free=${SPARROW_TEST_MIN_FREE:-4294967296}
        archive_dir=${SPARROW_TEST_ARCHIVE_DIR:-}
        if [[ -n $archive_dir ]]; then
            [[ $archive_dir =~ ^/[A-Za-z0-9._/+@:=,-]+$ ]] \
                || die "SPARROW_TEST_ARCHIVE_DIR must be an absolute path of letters, digits and . _ / + @ : = , - only (the path goes into a unit file and a sed command): $archive_dir"
            [[ /$archive_dir/ != */../* ]] || die "SPARROW_TEST_ARCHIVE_DIR must not contain '..': $archive_dir"
            [[ -d $archive_dir ]] \
                || die "the archive directory $archive_dir does not exist; the daemon never creates it, so create it on its own mount first (for example mkdir -m 0700 /dev/shm/NAME)"
            [[ -w $archive_dir && -x $archive_dir ]] || die "the archive directory $archive_dir is not writable by $(id -un), the daemon's user"
            archive_dir=$(cd -- "$archive_dir" && pwd -P)
            # SCRATCH may not exist yet; its nearest existing parent is on the device it will be on.
            probe=$SCRATCH
            while [[ ! -e $probe ]]; do probe=$(dirname -- "$probe"); done
            if [[ $(stat -c %d -- "$archive_dir") == "$(stat -c %d -- "$probe")" ]]; then
                die "the archive directory $archive_dir is on the same device as the scratch tree ($SCRATCH): the daemon refuses an archive that is not on its own mount, and an archive beside the store is no separate copy. Use a directory on its own mount, such as one under /dev/shm"
            fi
            case $archive_dir/ in
                "$SCRATCH"/*) die "the archive directory $archive_dir lies inside the scratch tree $SCRATCH" ;;
            esac
            require_own_mount "$archive_dir"
            # The daemon adopts only an empty directory, and purge removes what is recorded here, so a
            # directory that holds anything may be another store's archive. Only this instance's own
            # directory, already recorded with its own store id, may be set up again.
            if [[ -n $(find "$archive_dir" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
                own=$(own_store_id)
                if [[ ! -f $ARCHIVE_RECORD || $(<"$ARCHIVE_RECORD") != "$archive_dir" || -z $own || ! -f $archive_dir/STORE_ID || $(<"$archive_dir/STORE_ID") != "$own" ]]; then
                    die "the archive directory $archive_dir is not empty, so it may hold another store's archive, which the daemon refuses and which purge must never remove; use an empty directory (it holds: $(find "$archive_dir" -mindepth 1 -maxdepth 1 -printf '%f ' | head -c 200))"
                fi
            fi
        fi
        install -d -m 0700 -- "$SCRATCH" "$DATA" "$DATA/intake" "$DATA/intake/deploytest"
        # The daemon requires this directory to be a real directory owned by its account, with the
        # sender group and mode exactly 2750 (docs/deployment.md, "Ingest-only group socket").
        install -d -m 0700 -- "$INGEST"
        chgrp -- "$gid" "$INGEST" "$SCRATCH" || die "cannot give $SCRATCH to group $group; the invoking user must belong to it"
        chmod 2750 -- "$INGEST"
        # A sender must traverse SCRATCH to reach the socket: the group gets search only, so it can
        # neither list the scratch tree nor read the store (DATA, the store and ROOT stay 0700).
        chmod 0710 -- "$SCRATCH"
        bundle=$(find "$SPARROW_BIN" -mindepth 1 -maxdepth 1 -type d -name 'lexicons-*' | head -n 1)
        [[ -n $bundle ]] || die "no lexicon bundle in $SPARROW_BIN; run make install-bin"
        lexicons=$ROOT/usr/local/share/sparrow/${bundle##*/}
        # Written first, so install.sh keeps it instead of the production template and its secret.
        install -d -m 0750 -- "$ROOT/etc/sparrow"
        cat >"$CONFIG" <<CONFIG
# Test instance (deploy/test-instance.sh): scratch store and socket, no TCP listener.
socket_path = "$DATA/store/rpc.sock"
data_dir = "$DATA/store"
instance_role = "scratch"
ingest_socket_path = "$INGEST/ingest.sock"
ingest_socket_gid = $gid
disk_budget = $disk_budget
min_free_bytes = $min_free
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
        read_write_paths="$DATA $INGEST"
        if [[ -z $archive_dir && -f $ARCHIVE_RECORD ]]; then
            echo "test-instance.sh: dropping the archive directory recorded by an earlier setup ($(<"$ARCHIVE_RECORD")); it and what the daemon wrote there are left in place"
            rm -f -- "$ARCHIVE_RECORD"
        fi
        if [[ -n $archive_dir ]]; then
            printf '\n[archive]\ndir = "%s"\n' "$archive_dir" >>"$CONFIG"
            printf '%s\n' "$archive_dir" >"$ARCHIVE_RECORD"
            read_write_paths="$read_write_paths $archive_dir"
        fi
        SPARROW_DESTDIR=$ROOT "$DEPLOY_DIR/install.sh" --no-enable
        chmod 0700 -- "$ROOT"
        install -d -m 0700 -- "$UNIT_DIR"
        sed -E \
            -e '/^(User|Group|LoadCredential|AppArmorProfile|AmbientCapabilities|CapabilityBoundingSet|Slice|Nice|OOMScoreAdjust|ManagedOOMPreference|RequiresMountsFor)=/d' \
            -e "s|^ExecStart=.*|ExecStart=$ROOT/usr/local/libexec/sparrow/sparrowd --config $ROOT/etc/sparrow/server.toml|" \
            -e "s|^WorkingDirectory=.*|WorkingDirectory=$DATA|" \
            -e "s|^ReadWritePaths=.*|ReadWritePaths=$read_write_paths|" \
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
        # While the unit still answers, save the store's id: it is how purge tells this instance's archive
        # from another store's after the unit is gone.
        if systemctl --user is-active --quiet "$UNIT" 2>/dev/null && [[ -x $ROOT/usr/local/bin/sparrow ]]; then
            running=$("$ROOT/usr/local/bin/sparrow" "$DATA/store/rpc.sock" status 2>/dev/null | grep -o '"store_id":"store:[0-9a-f]\{32\}"' | head -n 1 | cut -d'"' -f4 || true)
            [[ -z $running ]] || printf '%s\n' "$running" >"$STORE_RECORD"
        fi
        systemctl --user stop "$UNIT" 2>/dev/null || true
        rm -f -- "$UNIT_DIR/$UNIT"
        systemctl --user daemon-reload
        echo "test-instance.sh: $UNIT removed; $SCRATCH and its store kept; 'test-instance.sh purge' removes the store"
        ;;
    purge)
        [[ $SCRATCH == /* ]] || { echo "test-instance.sh: SCRATCH must be absolute" >&2; exit 2; }
        grep -qx '# Test instance (deploy/test-instance.sh): scratch store and socket, no TCP listener.' "$CONFIG" 2>/dev/null \
            || die "$SCRATCH holds no configuration written by test-instance.sh ($CONFIG); nothing is removed"
        ! systemctl --user is-active --quiet "$UNIT" || die "$UNIT is active; run teardown first"
        [[ ! -e $UNIT_DIR/$UNIT ]] || die "$UNIT_DIR/$UNIT still exists; run teardown first"
        # Only the directory setup recorded, and in it only what the daemon writes: STORE_ID and the
        # <store_id>/ subtree it names. The directory and anything else in it stay.
        if [[ -f $ARCHIVE_RECORD ]]; then
            archive_dir=$(<"$ARCHIVE_RECORD")
            [[ $archive_dir == /* && $archive_dir != *..* ]] || die "$ARCHIVE_RECORD names no absolute archive directory ($archive_dir); nothing is removed"
            if [[ ! -d $archive_dir ]]; then
                echo "test-instance.sh: the recorded archive directory $archive_dir no longer exists (a tmpfs is gone after a reboot); nothing there to remove"
            elif [[ -f $archive_dir/STORE_ID ]]; then
                archive_store=$(<"$archive_dir/STORE_ID")
                [[ $archive_store =~ ^store:[0-9a-f]{32}$ ]] \
                    || die "$archive_dir/STORE_ID holds '$archive_store', not a store id; nothing is removed"
                own=$(own_store_id)
                [[ -n $own ]] \
                    || die "this instance's store id is unknown (no $STORE_RECORD, which teardown writes while the unit runs, and no journal line), so $archive_dir/STORE_ID ($archive_store) cannot be shown to be this instance's; nothing is removed. Delete $ARCHIVE_RECORD to leave the archive in place and purge the rest"
                [[ $own == "$archive_store" ]] \
                    || die "$archive_dir/STORE_ID names $archive_store but this instance's store is $own: that archive belongs to another store; nothing is removed. Delete $ARCHIVE_RECORD to leave it in place and purge the rest"
                [[ ! -e $archive_dir/$archive_store || ( -d $archive_dir/$archive_store && ! -L $archive_dir/$archive_store ) ]] \
                    || die "$archive_dir/$archive_store is not a directory; nothing is removed"
                rm -rf -- "${archive_dir:?}/${archive_store:?}"
                rm -f -- "$archive_dir/STORE_ID"
                echo "test-instance.sh: archive subtree $archive_dir/$archive_store and $archive_dir/STORE_ID removed; $archive_dir kept"
            else
                echo "test-instance.sh: $archive_dir holds no STORE_ID, so the archive worker wrote nothing there; $archive_dir kept"
            fi
        fi
        rm -rf -- "$DATA" "$INGEST" "$ROOT"
        rm -f -- "$ARCHIVE_RECORD" "$STORE_RECORD"
        echo "test-instance.sh: scratch store, intake, ingest directory and staged files in $SCRATCH removed"
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
