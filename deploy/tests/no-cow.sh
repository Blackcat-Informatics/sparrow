#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

set -euo pipefail
export PATH="/usr/bin:/bin"
# shellcheck source-path=SCRIPTDIR source=../lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/../lib.sh"
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
on_btrfs "$scratch" || die "set TMPDIR to a scratch directory on btrfs"
mv --exchange --no-copy -T --help >/dev/null 2>&1 || die "GNU mv with --exchange and --no-copy is required"

no_cow_swap_never_leaves_data_absent() {
    local data="$scratch/interrupted" partial inode status=0
    partial=$data.no-cow.partial
    mkdir -- "$data"
    chattr -C -- "$data"
    printf '%s\n' original > "$data/record"
    inode=$(stat -c %i -- "$data")
    (
        # convert_store_no_cow calls this stub through the sourced library.
        # shellcheck disable=SC2329
        mv() {
            local -a args=("$@")
            if [[ $1 != --exchange && ${args[-2]} == "$partial" ]]; then
                # Simulate interruption while retaining the exchanged-out original.
                return 42
            fi
            command mv "$@"
        }
        convert_store_no_cow "$data"
    ) > "$scratch/interrupted.log" 2>&1 || status=$?
    [[ $status == 1 ]]
    grep -F 'retaining the original did not report success' "$scratch/interrupted.log"
    [[ -d $data && -d $partial ]]
    [[ $(stat -c %i -- "$data") != "$inode" ]]
    [[ $(stat -c %i -- "$partial") == "$inode" ]]
    cmp -- "$data/record" "$partial/record"
    no_copy_on_write "$data"
    ! no_copy_on_write "$partial"
}

prepare_restart_fixture() {
    local root=$1
    mkdir -p -- "$root/bin" "$root/images" "$root/data"
    chattr -C -- "$root/data"
    cat > "$root/images/sparrowd-schema74-fixture" <<'IMAGE'
#!/usr/bin/env bash
printf '%s\n' 'usage: sparrowd --config FILE'
IMAGE
    cat > "$root/images/sparrow-schema74-fixture" <<'IMAGE'
#!/usr/bin/env bash
if (($# == 0)); then
    printf '%s\n' 'usage: sparrow'
else
    printf '%s\n' '{"store_id":"fixture","schema_version":74,"pressure":{"phase":"normal"},"consume_watches":[],"durability":{"failure":null}}'
fi
IMAGE
    chmod +x -- "$root/images/"*
    printf 'data_dir = "%s/data"\nsocket_path = "%s/data/rpc.sock"\n' "$root" "$root" > "$root/server.toml"
}

run_restart_fixture() {
    local root=$1
    env -i PATH="$root/bin:$PATH" SPARROW_FIXTURE_ROOT="$root" \
        SPARROW_UNIT=sparrow-fixture.service SPARROW_SCOPE=system \
        SPARROW_PREFIX="$root/prefix" SPARROW_CONFIG="$root/server.toml" SPARROW_BIN="$root/images" \
        timeout 30 bash "$DEPLOY_DIR/restart.sh" --rev fixture > "$root/restart.log" 2>&1
}

restart_refuses_conversion_before_stopping_the_daemon() {
    local root="$scratch/preflight" status=0
    prepare_restart_fixture "$root"
    mkdir -- "$root/data.no-cow.partial"
    cat > "$root/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SPARROW_FIXTURE_ROOT/systemctl.log"
case "$*" in
    "show -p ActiveState --value sparrow-fixture.service") printf '%s\n' active ;;
    *) exit 71 ;;
esac
STUB
    chmod +x -- "$root/bin/systemctl"
    run_restart_fixture "$root" || status=$?
    [[ $status == 1 ]]
    grep -F '.no-cow.partial is left from an earlier conversion' "$root/restart.log"
    [[ $(<"$root/systemctl.log") == 'show -p ActiveState --value sparrow-fixture.service' ]]
    [[ ! -e $root/prefix ]]
}

no_cow_conversion_keeps_downtime_to_the_delta() {
    local root="$scratch/delta" copies
    prepare_restart_fixture "$root"
    printf 'stable\n' > "$root/data/stable"
    printf 'before\n' > "$root/data/changed"
    printf 'remove\n' > "$root/data/removed"
    mkdir -p -- "$root/prefix/libexec/sparrow"
    ln -s -- "$root/images/sparrow-schema74-fixture" "$root/prefix/libexec/sparrow/sparrow"
    cat > "$root/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
root=$SPARROW_FIXTURE_ROOT
case "$*" in
    "show -p ActiveState --value sparrow-fixture.service" | "is-active sparrow-fixture.service") printf '%s\n' active ;;
    "show -p Result --value sparrow-fixture.service") printf '%s\n' success ;;
    "show -p MainPID --value sparrow-fixture.service") printf '%s\n' 123 ;;
    "is-active --quiet sparrow-fixture.service") ;;
    "stop sparrow-fixture.service")
        [[ -f $root/precopy.stats ]]
        date +%s%N > "$root/stopped"
        printf 'after!\n' > "$root/data/changed"
        touch -r "$root/data.no-cow.partial/changed" "$root/data/changed"
        printf 'added\n' > "$root/data/added"
        unlink -- "$root/data/removed"
        ;;
    "start sparrow-fixture.service")
        [[ -f $root/delta.stats && ! -e $root/data.no-cow.partial ]]
        date +%s%N > "$root/started"
        ;;
    *) exit 71 ;;
esac
STUB
    cat > "$root/bin/rsync" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
root=$SPARROW_FIXTURE_ROOT
if [[ -f $root/stopped ]]; then
    LC_ALL=C /usr/bin/rsync --stats "$@" > "$root/delta.stats"
else
    LC_ALL=C /usr/bin/rsync --stats "$@" > "$root/precopy.stats"
    stat -c '%d:%i' -- "$root/data.no-cow.partial/stable" > "$root/stable.inode"
fi
STUB
    chmod +x -- "$root/bin/systemctl" "$root/bin/rsync"
    run_restart_fixture "$root" || { cat -- "$root/restart.log" >&2; return 1; }
    grep -F 'Number of regular files transferred: 3' "$root/precopy.stats"
    grep -F 'Number of regular files transferred: 2' "$root/delta.stats"
    grep -F 'Total transferred file size: 13 bytes' "$root/delta.stats"
    [[ $(stat -c '%d:%i' -- "$root/data/stable") == "$(<"$root/stable.inode")" ]]
    [[ ! -e $root/data/removed ]]
    copies=("$root/data".cow-*)
    [[ ${#copies[@]} == 1 ]]
    store_listings_match "$root/data" "${copies[0]}"
    store_file_bytes_match "$root/data" "${copies[0]}"
    printf 'Scratch restart stopped interval: %s ns\n' "$(( $(<"$root/started") - $(<"$root/stopped") ))"
}

precopy_refuses_a_replaced_source() {
    local data="$scratch/replaced" status=0
    mkdir -- "$data"
    chattr -C -- "$data"
    printf 'original\n' > "$data/record"
    (
        # shellcheck disable=SC2329
        rsync() {
            command rsync "$@" || return
            mv -T -- "$data" "$data.original"
            mkdir -- "$data"
            printf 'replacement\n' > "$data/record"
        }
        precopy_store_no_cow "$data"
    ) > "$scratch/replaced.log" 2>&1 || status=$?
    [[ $status == 1 ]]
    grep -F 'changed after the pre-copy' "$scratch/replaced.log"
    cmp -- "$data.original/record" "$data.no-cow.partial/record"
    [[ $(<"$data/record") == replacement ]]
}

no_cow_conversion_handles_a_newline_in_a_file_name() {
    local data="$scratch/newline" name=$'sub\nfolder/first\nsecond'
    local kept
    mkdir -- "$data"
    chattr -C -- "$data"
    mkdir -- "$data/${name%/*}"
    printf 'evidence\000with a newline\n' > "$data/$name"
    convert_store_no_cow "$data" > "$scratch/newline.log"
    grep -F '1 files with copy-on-write off' "$scratch/newline.log"
    local copies=("$data".cow-*)
    [[ ${#copies[@]} == 1 ]]
    kept=${copies[0]}
    cmp -- "$data/$name" "$kept/$name"
    [[ $(copy_on_write_files "$data") == 0 ]]
    [[ $(copy_on_write_files "$kept") == 1 ]]
}

no_cow_swap_never_leaves_data_absent
restart_refuses_conversion_before_stopping_the_daemon
no_cow_conversion_handles_a_newline_in_a_file_name
no_cow_conversion_keeps_downtime_to_the_delta
precopy_refuses_a_replaced_source
