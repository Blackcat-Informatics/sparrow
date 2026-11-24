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

restart_refuses_conversion_before_stopping_the_daemon() {
    local root="$scratch/preflight" status=0
    mkdir -p -- "$root/bin" "$root/images" "$root/data" "$root/data.no-cow.partial"
    chattr -C -- "$root/data"
    cat > "$root/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SPARROW_STUB_LOG"
case "$*" in
    "show -p ActiveState --value sparrow-fixture.service") printf '%s\n' active ;;
    *) exit 71 ;;
esac
STUB
    cat > "$root/images/sparrowd-schema74-fixture" <<'IMAGE'
#!/usr/bin/env bash
printf '%s\n' 'usage: sparrowd --config FILE'
IMAGE
    cat > "$root/images/sparrow-schema74-fixture" <<'IMAGE'
#!/usr/bin/env bash
printf '%s\n' 'usage: sparrow'
IMAGE
    chmod +x -- "$root/bin/systemctl" "$root/images/"*
    printf 'data_dir = "%s/data"\nsocket_path = "%s/data/rpc.sock"\n' "$root" "$root" > "$root/server.toml"
    env -i PATH="$root/bin:$PATH" SPARROW_STUB_LOG="$root/systemctl.log" \
        SPARROW_UNIT=sparrow-fixture.service SPARROW_SCOPE=system \
        SPARROW_PREFIX="$root/prefix" SPARROW_CONFIG="$root/server.toml" SPARROW_BIN="$root/images" \
        timeout 15 bash "$DEPLOY_DIR/restart.sh" --rev fixture > "$root/restart.log" 2>&1 || status=$?
    [[ $status == 1 ]]
    grep -F '.no-cow.partial is left from an earlier conversion' "$root/restart.log"
    [[ $(<"$root/systemctl.log") == 'show -p ActiveState --value sparrow-fixture.service' ]]
    [[ ! -e $root/prefix ]]
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
