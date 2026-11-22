#!/usr/bin/env bash
# Runs a full-disk durability test on a tmpfs of SPARROW_ENOSPC_MIB MiB (default 160, which leaves
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
#
# The log receives the test output and ends with EXIT=; the RocksDB LOG is copied beside it as
# LOG-<name of the log>.
set -euo pipefail
log=${1:?usage: tools/enospc-check.sh LOG core|worker [ARM]}
mode=${2:?usage: tools/enospc-check.sh LOG core|worker [ARM]}
case "$mode" in
  core) package=sparrow-core; selector="--test enospc"; target=enospc ;;
  worker) package=sparrow-server; selector="--bin sparrowd"; target=sparrowd ;;
  *) echo "unknown mode $mode" >&2; exit 2 ;;
esac
export SPARROW_ENOSPC_ARM=${3:-B}
cargo test --locked --release -p "$package" $selector --no-run > "$log" 2>&1
bin=$(cargo test --locked --release -p "$package" $selector --no-run --message-format=json 2>/dev/null \
  | TARGET=$target python3 -I -c 'import json,os,sys
for l in sys.stdin:
    m=json.loads(l)
    if m.get("executable") and m.get("target",{}).get("name")==os.environ["TARGET"] and m.get("profile",{}).get("test"): print(m["executable"])')
filter=a_full_disk_latch
timeout 600 unshare -rm sh -c '
  set -eu
  d=$(mktemp -d -- "$(dirname "$2")/enospc.XXXXXX")
  mount -t tmpfs -o size=${SPARROW_ENOSPC_MIB:-160}m tmpfs "$d"
  status=0
  SPARROW_ENOSPC_DIR="$d" "$1" "$4" --ignored --nocapture || status=$?
  for native in "$d"/store/LOG*; do
    [ -f "$native" ] || continue
    cp "$native" "$(dirname "$2")/$(basename "$native")-$(basename "$2" .log)" || {
      [ "$status" -ne 0 ] || status=1
    }
  done
  exit "$status"
' sh "$bin" "$log" x "$filter" >> "$log" 2>&1 && status=0 || status=$?
echo "EXIT=$status" >> "$log"
exit "$status"
