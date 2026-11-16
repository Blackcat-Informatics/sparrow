#!/usr/bin/env bash
# Usage: run-logged.sh <logname> <cmd...>; runs in the worktree, records provenance and EXIT inside the log.
set -u
W=/home/paudley/Active/sparrow/.worktrees/328-archive-prepare-read-renews-its-lease
log=$W/.stage/archive-prepare-read-renews-its-lease/logs/$1
shift
cd "$W" || exit 2
{ echo "pwd=$(pwd)"; echo "HEAD=$(git rev-parse HEAD)"; date; echo "cmd=$*"; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; } > "$log"
"$@" >> "$log" 2>&1
st=$?
{ date; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; echo "EXIT=$st"; } >> "$log"
exit $st
