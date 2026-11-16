#!/bin/bash
# usage: run.sh LOGNAME cmd args...   (run under flock by the caller)
cd /home/paudley/Active/sparrow/.worktrees/323-index-artifacts-by-payload-id-the-store || exit 99
log=.stage/index-artifacts-by-payload-id-the-store/logs/$1.log
shift
hdr() {
  pwd
  git rev-parse HEAD
  stat -c '%y' /home/paudley/stage/scripts/cargo
  sha256sum /home/paudley/stage/scripts/cargo
}
{
  hdr
  echo "CMD: $*"
  echo "LOAD_START: $(cat /proc/loadavg)"
  "$@"
  rc=$?
  echo "LOAD_END: $(cat /proc/loadavg)"
  hdr
  echo EXIT=$rc
} > "$log" 2>&1
exit $rc
