#!/bin/bash
# usage: run.sh LOGNAME cmd...   (run under flock by the caller)
log=/home/paudley/Active/sparrow/.worktrees/332-archive-worker-writes-store-id-on-its/.stage/archive-worker-writes-store-id-on-its/$1.log
shift
stamp() {
  {
    echo "pwd=$(pwd)"
    echo "HEAD=$(git rev-parse HEAD)"
    echo "cargo shim: $(stat -c %y /home/paudley/stage/scripts/cargo) $(sha256sum /home/paudley/stage/scripts/cargo)"
  } >>"$log"
}
: >"$log"
echo "== start $(date)" >>"$log"
stamp
"$@" >>"$log" 2>&1
status=$?
echo "== end $(date)" >>"$log"
stamp
echo "EXIT=$status" >>"$log"
exit $status
