#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
# Runs a disk-pressure test on a tmpfs of SPARROW_ENOSPC_MIB MiB (default 160, which leaves
# RocksDB's 32 MiB recovery buffer free once the filler is removed) mounted in a user and mount
# namespace, so no root and no change outside the namespace is needed (#447). The tests need real
# time (RocksDB's 5 s space poll) and are ignored in the gate. Run from the checkout root under
# the shared cargo lock.
#
#   tools/enospc-check.sh LOG core [A|B]   the store alone: arm A repeats the proof of durability
#                                          every second after space returns, arm B leaves the
#                                          store alone and proves once at the end
#   tools/enospc-check.sh LOG core A0-reopen   reproduce the native wedge and reopen the store
#   tools/enospc-check.sh LOG worker       the daemon's maintenance worker under the same fault
#   tools/enospc-check.sh LOG floor        the running daemon crosses the write floor and recovers
#
# The log contains the test output and exit status. For core and worker tests, the
# RocksDB LOG is copied beside it as LOG-<name of the log>.
set -euo pipefail
log=${1:?usage: tools/enospc-check.sh LOG core|worker|floor [ARM]}
mode=${2:?usage: tools/enospc-check.sh LOG core|worker|floor [ARM]}
filter=a_full_disk_latch
case "$mode" in
  core) package=sparrow-core; selector=(--test enospc); target=enospc ;;
  worker) package=sparrow-server; selector=(--bin sparrowd); target=sparrowd ;;
  floor) package=sparrow-rpc-tests; selector=(--test rpc); target=rpc
         filter=runtime_free_space_pauses_writes_and_resumes_consumption ;;
  *) echo "unknown mode $mode" >&2; exit 2 ;;
esac
export SPARROW_ENOSPC_ARM=${3:-B}
cargo test --locked --release -p "$package" "${selector[@]}" --no-run > "$log" 2>&1
bin=$(cargo test --locked --release -p "$package" "${selector[@]}" --no-run --message-format=json 2>/dev/null \
  | TARGET=$target python3 -I -c 'import json,os,sys
for l in sys.stdin:
    m=json.loads(l)
    if m.get("executable") and m.get("target",{}).get("name")==os.environ["TARGET"] and m.get("profile",{}).get("test"): print(m["executable"])')
log_dir=$(cd -- "$(dirname -- "$log")" && pwd -P)
log_name=$(basename -- "$log" .log)
test_args=("$filter" --ignored --nocapture)
if [[ $mode == floor ]]; then
  test_args+=(--exact)
fi
if timeout 600 unshare -rm sh -s -- "$bin" "$log_dir" "$log_name" "${test_args[@]}" >> "$log" 2>&1 <<'SH'
  set -eu
  bin=$1 log_dir=$2 log_name=$3
  shift 3
  d=$(mktemp -d -- "$log_dir/enospc.XXXXXX")
  mount -t tmpfs -o size=${SPARROW_ENOSPC_MIB:-160}m tmpfs "$d"
  status=0
  SPARROW_ENOSPC_DIR="$d" "$bin" "$@" || status=$?
  for native in "$d"/store/LOG*; do
    [ -f "$native" ] || continue
    cp "$native" "$log_dir/$(basename "$native")-$log_name" || {
      [ "$status" -ne 0 ] || status=1
    }
  done
  exit "$status"
SH
then
  status=0
else
  status=$?
fi
if [[ $mode == floor && $status == 0 ]] && ! grep -q '^test result: ok[.] 1 passed; 0 failed;' "$log"; then
  echo "the runtime free-space test did not run" >> "$log"
  status=1
fi
echo "EXIT=$status" >> "$log"
exit "$status"
